"""Phase 1 — Floodscan benchmark extraction for all Niger/Benue states.

For each state, extracts daily Floodscan SFED at the grid pixels within its
main-channel river buffer (10 km) ∩ riverine LGAs, 1998–2025. This is the flood
ground truth the per-state trigger workflow (Phase 2) calibrates against.

Efficient approach: the SFED grid is fixed, so per-state pixel masks are computed
once from a sample COG; each daily COG is then windowed to the Nigeria AOI and the
masked pixels extracted (threaded I/O). Monitoring is year-round (no wet-season
restriction), so all dates are kept.

    python pipelines/build_floodscan_benchmark.py --sample-year 2022   # quick test
    python pipelines/build_floodscan_benchmark.py                      # full run

Outputs (one per state, matching the existing fs_*_pixels schema date,y,x,SFED):
    ds-aa-nga-flooding/processed/floodscan/fs_{state}_pixels_1998_2025.parquet
"""

import argparse
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd
import rioxarray  # noqa: F401 — registers .rio accessor
from dotenv import load_dotenv
from shapely.geometry import box
from tqdm import tqdm

load_dotenv()
warnings.filterwarnings("ignore")

import ocha_stratus as stratus  # noqa: E402

from src.config import load_lga_registry  # noqa: E402
from src.constants import PROJECT_PREFIX  # noqa: E402
from src.datasources import hydrosheds  # noqa: E402

BUFFER_KM = 10
DIS_MIN_CMS = 500
COG_FMT = "floodscan/daily/v5/processed/aer_area_300s_v{date}_v05r01.tif"
START = "1998-01-12"
END = "2025-12-31"
AOI = (2.5, 4.0, 15.0, 14.0)  # Nigeria bbox for windowed reads
FS_BLOB_FMT = PROJECT_PREFIX + "/processed/floodscan/fs_{state}_pixels_1998_2025.parquet"


def _read_cog(date):
    """Open one daily SFED COG windowed to the Nigeria AOI; None if missing."""
    try:
        da = stratus.open_blob_cog(
            COG_FMT.format(date=date), stage="prod", container_name="raster"
        ).sel(band=1)
        return da.rio.clip_box(*AOI)
    except Exception:
        return None


def build_pixel_masks(lga_reg, gdf_lga):
    """Per-state pixel→(state,pcode) table from a sample COG's grid."""
    rivers = hydrosheds.load_selected_rivers()
    main = rivers[rivers["DIS_AV_CMS"] >= DIS_MIN_CMS]
    river_buf = main.to_crs("EPSG:3857").buffer(
        BUFFER_KM * 1000).to_crs(4326).union_all()

    sample = _read_cog("2022-09-20")
    sample = sample.rio.clip([river_buf])  # restrict to river buffer

    rows = []
    riverine = gdf_lga[gdf_lga["ADM2_PCODE"].isin(set(lga_reg["pcode"]))]
    state_of = lga_reg.set_index("pcode")["state"].to_dict()
    for pcode, geom in riverine.set_index("ADM2_PCODE")["geometry"].items():
        try:
            clip = sample.rio.clip([geom], all_touched=True)
        except Exception:
            continue
        df = clip.to_dataframe(name="SFED").reset_index().dropna(subset=["SFED"])
        if df.empty:
            continue
        df = df[["x", "y"]].drop_duplicates()
        df["pcode"] = pcode
        df["state"] = state_of[pcode]
        rows.append(df)
    pix = pd.concat(rows, ignore_index=True)
    pix["x"] = pix["x"].round(4)
    pix["y"] = pix["y"].round(4)
    # keep per-state pixel sets independent — a boundary pixel touched by two
    # states' LGAs informs BOTH (matches the original per-state clipping and the
    # endorsed 42-pixel Adamawa benchmark). Dedup only within a state.
    pix = pix.drop_duplicates(subset=["state", "x", "y"])
    return pix


def extract_date(date, master_xy):
    """Extract SFED at the master pixel set for one date."""
    da = _read_cog(date)
    if da is None:
        return None
    df = da.to_dataframe(name="SFED").reset_index()[["x", "y", "SFED"]]
    df["x"] = df["x"].round(4)
    df["y"] = df["y"].round(4)
    out = master_xy.merge(df, on=["x", "y"], how="inner")
    out["date"] = pd.Timestamp(date)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", default="dev", choices=["dev", "prod"])
    p.add_argument("--sample-year", type=int, default=None,
                   help="restrict to one year for a quick validation run")
    p.add_argument("--states", nargs="*", default=None,
                   help="restrict to these states (default: all)")
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args()

    print("Loading registry + boundaries…", flush=True)
    lga_reg = load_lga_registry()
    if args.states:
        lga_reg = lga_reg[lga_reg["state"].isin(args.states)]
    gdf_lga = stratus.codab.load_codab_from_blob("NGA", admin_level=2)

    print("Building per-state pixel masks…", flush=True)
    pix = build_pixel_masks(lga_reg, gdf_lga)
    master_xy = pix[["x", "y", "state", "pcode"]]
    print(f"  {len(master_xy)} pixels across {pix['state'].nunique()} states",
          flush=True)
    print(pix.groupby("state").size().to_string(), flush=True)

    dates = pd.date_range(START, END)
    if args.sample_year:
        dates = dates[dates.year == args.sample_year]
    date_strs = [d.date().isoformat() for d in dates]
    print(f"\nExtracting {len(date_strs)} dates with {args.workers} workers…",
          flush=True)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(extract_date, d, master_xy): d for d in date_strs}
        for f in tqdm(as_completed(futs), total=len(futs)):
            r = f.result()
            if r is not None and not r.empty:
                results.append(r)

    allpix = pd.concat(results, ignore_index=True)
    print(f"\nExtracted {len(allpix):,} pixel-days.", flush=True)

    for state, sub in allpix.groupby("state"):
        out = sub[["date", "y", "x", "SFED"]].sort_values(["date", "y", "x"])
        blob = FS_BLOB_FMT.format(state=state.lower().replace(" ", "_"))
        n_days = out["date"].nunique()
        print(f"  {state}: {len(out):,} rows, {out[['x','y']].drop_duplicates().shape[0]} "
              f"pixels, {n_days} days -> {blob}", flush=True)
        if not args.no_upload:
            stratus.upload_parquet_to_blob(out, blob, stage=args.stage)

    print("Done." if not args.no_upload else "Done (no upload).", flush=True)


if __name__ == "__main__":
    main()
