"""Build the multi-state config registry (LGAs + forecast gauges/stations).

Derives, for the 14 Niger/Benue states, the riverine LGAs (main-channel river
buffer ∩ LGA boundaries), enriches them with basin, NiHSA-HFR membership and
Hannah's Google-coverage correlation, and assembles the candidate gauge/station
pool. Uploads both registries to blob for the analysis workflow, the web-data
generator, and the monitoring app to read.

    python pipelines/build_config_registry.py            # writes to dev stage
    python pipelines/build_config_registry.py --stage prod

Outputs (container 'projects'):
    ds-aa-nga-flooding/processed/config/lga_registry.parquet
    ds-aa-nga-flooding/processed/config/gauge_registry.parquet

Gauge skill columns (best_r, best_lag, rp_threshold, lead_time) are left null
here and filled by the per-state trigger workflow.
"""

import argparse
import warnings

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

import ocha_stratus as stratus  # noqa: E402

from src.config.presets import DEFAULT_CONSENSUS_FRAC  # noqa: E402
from src.config.registry import (  # noqa: E402
    GAUGE_REGISTRY_BLOB,
    LGA_REGISTRY_BLOB,
    STATE_PARAMS_BLOB,
)
from src.datasources import grrr, hydrosheds  # noqa: E402
from src.datasources.glofas import GF_STATIONS  # noqa: E402

TARGET_STATES = [
    "Adamawa", "Anambra", "Bayelsa", "Benue", "Delta", "Edo", "Kebbi",
    "Kogi", "Kwara", "Nasarawa", "Niger", "Rivers", "Taraba", "Imo",
]
BUFFER_KM = 10
DIS_MIN_CMS = 500  # isolate Niger+Benue main channels from minor tributaries
# A gauge informs every state whose riverine-LGA zone, buffered by this margin,
# contains it — so a gauge on the far bank of a border river serves both states
# (the gauge registry is therefore keyed by (gauge_id, state), not gauge_id).
STATE_GAUGE_BUFFER_KM = 10
BASINS = {  # HydroBASINS L4 id -> (name, river_system); matches the R pilot
    1040909900: ("Benue", "Benue"),
    1040760290: ("Upper Niger", "Niger"),
    1040909890: ("Lower Niger", "Niger"),
    1040022420: ("Niger Delta", "Niger"),
}
ADAMAWA_ENDORSED = {  # the 7 endorsed 2025 framework LGAs (pinned)
    "NG002002", "NG002021", "NG002020", "NG002005",
    "NG002001", "NG002016", "NG002009",
}
# Per-state defaults for the state_params table (the "config levers").
# Monitoring is year-round (no wet-season restriction), per the 2026 decision.
ANALYSIS_START_YEAR = 1998   # Floodscan record start
ANALYSIS_END_YEAR = 2023     # overlap with GRRR reforecast evaluation
RP_TARGET = 4                # design return period (years)


def _river_buffer():
    rivers = hydrosheds.load_selected_rivers()
    main = rivers[rivers["DIS_AV_CMS"] >= DIS_MIN_CMS].copy()
    buf = main.to_crs("EPSG:3857").buffer(BUFFER_KM * 1000).to_crs(4326)
    return main, buf.union_all()


def _assign_basin(gdf, basins4):
    """Assign each geometry to the basin its centroid falls in (nearest fallback)."""
    cen = gdf.copy()
    cen["geometry"] = cen.geometry.centroid
    j = gpd.sjoin(cen, basins4[["HYBAS_ID", "geometry"]], how="left",
                  predicate="within")
    j = j[~j.index.duplicated(keep="first")]
    hid = j["HYBAS_ID"]
    if hid.isna().any():
        b3857 = basins4.to_crs(3857)
        for idx in gdf.index[hid.isna()]:
            c = gpd.GeoSeries([cen.loc[idx].geometry], crs=4326).to_crs(3857).iloc[0]
            hid.loc[idx] = b3857.iloc[b3857.distance(c).values.argmin()]["HYBAS_ID"]
    return hid.astype("int64")


def build_lga_registry(gdf_lga, river_buf, basins4):
    adm1 = gdf_lga[["ADM1_EN", "ADM1_PCODE"]].drop_duplicates().set_index(
        "ADM1_EN")["ADM1_PCODE"].to_dict()
    target_pcodes = [adm1[s] for s in TARGET_STATES]

    hfr = set(stratus.load_geoparquet_from_blob(
        "ds-aa-nga-flooding/processed/selected_lgas.parquet")["ADM2_PCODE"])
    cov = stratus.load_parquet_from_blob(
        "ds-aa-nga-flooding/processed/web_map/google_best_r.parquet"
    ).set_index("ADM2_PCODE")["best_gauge_r"].to_dict()

    states = gdf_lga[gdf_lga["ADM1_PCODE"].isin(target_pcodes)].copy()
    riverine = states[states.intersects(river_buf)].copy()
    riverine["HYBAS_ID"] = _assign_basin(riverine, basins4)

    reg = pd.DataFrame({
        "pcode": riverine["ADM2_PCODE"],
        "name": riverine["ADM2_EN"],
        "state": riverine["ADM1_EN"],
        "basin": riverine["HYBAS_ID"].map(lambda x: BASINS[x][0]),
        "river_system": riverine["HYBAS_ID"].map(lambda x: BASINS[x][1]),
        "is_riverine": True,
        "is_hfr": riverine["ADM2_PCODE"].isin(hfr),
        "is_endorsed_adamawa": riverine["ADM2_PCODE"].isin(ADAMAWA_ENDORSED),
        "coverage_r": riverine["ADM2_PCODE"].map(cov).round(3),
        "floodscan_threshold": pd.NA,
    }).reset_index(drop=True)

    # pin Adamawa to the endorsed 7 (drop auto-added extras, e.g. Song)
    drop = (reg["state"] == "Adamawa") & (~reg["is_endorsed_adamawa"])
    if drop.any():
        print(f"  pinning Adamawa to endorsed 7 (dropping "
              f"{reg[drop]['name'].tolist()})")
    return reg[~drop].reset_index(drop=True)


def build_gauge_registry(lga_reg, gdf_lga, river_buf, basins4):
    riverine = gdf_lga[gdf_lga["ADM2_PCODE"].isin(set(lga_reg["pcode"]))][
        ["ADM2_PCODE", "ADM2_EN", "ADM1_EN", "geometry"]
    ].rename(columns={"ADM2_PCODE": "lga_pcode", "ADM2_EN": "lga_name",
                      "ADM1_EN": "state"})
    basin_of = lga_reg.set_index("pcode")["basin"].to_dict()

    # --- GRRR gauges on the main channel (search zone includes the cross-river
    # buffer so far-bank gauges just outside the riverine LGAs are in the pool)
    zone_gdf = riverine.copy().to_crs(3857)
    zone_gdf["geometry"] = zone_gdf.buffer(STATE_GAUGE_BUFFER_KM * 1000)
    zone_gdf = zone_gdf.to_crs(4326)
    g = grrr.get_gauges_by_area(zone_gdf, include_non_quality_verified=True)
    g = g[g["has_model"]].copy()
    g = g[g.intersects(river_buf)].copy()
    g["source"] = "grrr"
    g = g.rename(columns={"latitude": "lat", "longitude": "lon"})

    # --- GloFAS reference stations ---
    gf = gpd.GeoDataFrame(
        [{"gauge_id": k, "source": "glofas", "lat": v["lat"], "lon": v["lon"],
          "quality_verified": pd.NA} for k, v in GF_STATIONS.items()],
        geometry=gpd.points_from_xy(
            [v["lon"] for v in GF_STATIONS.values()],
            [v["lat"] for v in GF_STATIONS.values()]),
        crs=4326,
    )

    cols = ["gauge_id", "source", "lat", "lon", "quality_verified", "geometry"]
    allg = gpd.GeoDataFrame(pd.concat([g[cols], gf[cols]], ignore_index=True),
                            geometry="geometry", crs=4326).to_crs(3857)
    riv3857 = riverine.to_crs(3857)

    # one row per (gauge, state): a gauge informs every state whose riverine-LGA
    # zone (buffered STATE_GAUGE_BUFFER_KM) contains it, so a far-bank gauge on a
    # border river serves both sides. in_state=False marks the buffered-in rows.
    rows = []
    for state, sub in riv3857.groupby("state"):
        lga_union = sub.union_all()
        zone = lga_union.buffer(STATE_GAUGE_BUFFER_KM * 1000)
        within = allg[allg.within(zone)]
        if within.empty:
            continue
        nn = gpd.sjoin_nearest(within, sub[["lga_pcode", "lga_name", "geometry"]],
                               how="left")
        nn = nn[~nn.index.duplicated(keep="first")]
        for idx, r in nn.iterrows():
            rows.append({
                "gauge_id": r["gauge_id"],
                "source": r["source"],
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "state": state,
                "lga_pcode": r["lga_pcode"],
                "lga_name": r["lga_name"],
                "in_state": bool(within.loc[idx].geometry.within(lga_union)),
                "quality_verified": r["quality_verified"],
                "best_r": pd.NA,
                "best_lag": pd.NA,
                "rp_threshold": pd.NA,
                "lead_time": pd.NA,
            })
    reg = pd.DataFrame(rows)
    reg["basin"] = reg["lga_pcode"].map(basin_of)
    n_shared = (reg.groupby(reg["gauge_id"]).size() > 1).sum()
    print(f"  {reg['gauge_id'].nunique()} unique gauges -> {len(reg)} "
          f"(gauge, state) rows; {n_shared} gauges serve >1 state, "
          f"{(~reg['in_state']).sum()} rows buffered in from across the river")
    return reg.reset_index(drop=True)


def build_state_params(lga_reg, gauge_reg, gdf_lga):
    """One row per state: the scalar config levers (reference station, basins,
    analysis window, RP target, consensus fraction)."""
    adm1 = gdf_lga[["ADM1_EN", "ADM1_PCODE"]].drop_duplicates().set_index(
        "ADM1_EN")["ADM1_PCODE"].to_dict()
    riverine = gdf_lga[gdf_lga["ADM2_PCODE"].isin(set(lga_reg["pcode"]))]

    gf_pts = gpd.GeoDataFrame(
        [{"station": k, "lat": v["lat"], "lon": v["lon"]}
         for k, v in GF_STATIONS.items()],
        geometry=gpd.points_from_xy(
            [v["lon"] for v in GF_STATIONS.values()],
            [v["lat"] for v in GF_STATIONS.values()]),
        crs=4326,
    ).to_crs(3857)

    rows = []
    for state, sub in lga_reg.groupby("state"):
        geom = riverine[riverine["ADM1_EN"] == state].to_crs(3857)
        cen = geom.union_all().centroid
        d = gf_pts.distance(cen)
        stn = gf_pts.iloc[d.values.argmin()]
        gsrc = gauge_reg[gauge_reg["state"] == state]["source"]
        rows.append({
            "state": state,
            "adm1_pcode": adm1[state],
            "glofas_station": stn["station"],
            "glofas_station_lat": GF_STATIONS[stn["station"]]["lat"],
            "glofas_station_lon": GF_STATIONS[stn["station"]]["lon"],
            "basins": ", ".join(sorted(sub["basin"].unique())),
            "n_lgas": len(sub),
            "n_gauges_grrr": int((gsrc == "grrr").sum()),
            "n_gauges_glofas": int((gsrc == "glofas").sum()),
            "median_coverage_r": round(sub["coverage_r"].median(), 3)
            if sub["coverage_r"].notna().any() else None,
            "analysis_start_year": ANALYSIS_START_YEAR,
            "analysis_end_year": ANALYSIS_END_YEAR,
            "rp_target": RP_TARGET,
            "consensus_frac": DEFAULT_CONSENSUS_FRAC,
            "glofas_readiness_thresh": pd.NA,      # filled by Phase 3
            "glofas_readiness_leadtime": pd.NA,    # filled by Phase 3
        })
    return pd.DataFrame(rows).sort_values("state").reset_index(drop=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", default="dev", choices=["dev", "prod"])
    p.add_argument("--dry-run", action="store_true",
                   help="build but do not upload; print summaries only")
    args = p.parse_args()

    print("Loading boundaries, rivers, basins…")
    gdf_lga = stratus.codab.load_codab_from_blob("NGA", admin_level=2)
    main_river, river_buf = _river_buffer()
    basins = hydrosheds.load_basins(4)
    basins4 = basins[basins["HYBAS_ID"].isin(BASINS)].copy()

    print("Building LGA registry…")
    lga_reg = build_lga_registry(gdf_lga, river_buf, basins4)
    print(f"  {len(lga_reg)} LGAs across {lga_reg['state'].nunique()} states")

    print("Building gauge registry (querying Google Flood Hub)…")
    gauge_reg = build_gauge_registry(lga_reg, gdf_lga, river_buf, basins4)
    n_grrr = (gauge_reg["source"] == "grrr").sum()
    n_gf = (gauge_reg["source"] == "glofas").sum()
    print(f"  {len(gauge_reg)} gauges ({n_grrr} GRRR + {n_gf} GloFAS)")

    print("Building state params…")
    state_params = build_state_params(lga_reg, gauge_reg, gdf_lga)
    print(f"  {len(state_params)} states")

    print("\nLGAs by basin:\n",
          lga_reg.groupby("basin").size().to_string())
    print("\nState params:\n", state_params[[
        "state", "glofas_station", "basins", "n_lgas",
        "n_gauges_grrr", "n_gauges_glofas", "median_coverage_r",
    ]].to_string(index=False))

    if args.dry_run:
        print("\n[dry-run] not uploading.")
        return
    stratus.upload_parquet_to_blob(lga_reg, LGA_REGISTRY_BLOB, stage=args.stage)
    stratus.upload_parquet_to_blob(gauge_reg, GAUGE_REGISTRY_BLOB, stage=args.stage)
    stratus.upload_parquet_to_blob(state_params, STATE_PARAMS_BLOB, stage=args.stage)
    print(f"\nUploaded to blob (stage={args.stage}):"
          f"\n  {LGA_REGISTRY_BLOB}\n  {GAUGE_REGISTRY_BLOB}\n  {STATE_PARAMS_BLOB}")


if __name__ == "__main__":
    main()
