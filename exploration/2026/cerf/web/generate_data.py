#!/usr/bin/env python3
"""
Generate dashboard data for the CERF Nigeria trigger explorer.

Reads from workflow blob outputs (notebooks 01–05) and writes:
  data.json  — raw data for the dashboard

Usage (from repo root or this directory):
    python exploration/2026/cerf/web/generate_data.py

── Required blob inputs ──────────────────────────────────────────────────────

  Workflow outputs  [notebooks 01–05]
    ds-aa-nga-flooding/processed/workflow/{state}_floodscan_daily.parquet
    ds-aa-nga-flooding/processed/workflow/{state}_floodscan_annual.parquet
    ds-aa-nga-flooding/processed/workflow/{state}_gauge_correlations.parquet
    ds-aa-nga-flooding/processed/workflow/{state}_selected_gauges.parquet
    ds-aa-nga-flooding/processed/workflow/{state}_trigger_grid.parquet

  GloFAS reanalysis  [download_glofas_reanalysis.ipynb]
    ds-aa-nga-flooding/processed/glofas/glofas_reanalysis_wuroboki.parquet
    ds-aa-nga-flooding/processed/glofas/glofas_reanalysis_makurdi.parquet

  GloFAS reforecast  [download_glofas_reforecast.ipynb]
    ds-aa-nga-flooding/processed/glofas/wuroboki_glofas_reforecast_ens.parquet
    ds-aa-nga-flooding/processed/glofas/glofas_reforecast_makurdi_ensemble.parquet
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parents[4]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

import ocha_stratus as stratus  # noqa: E402
import pandas as pd  # noqa: E402

from src.constants import PROJECT_PREFIX, STATE_CONFIG  # noqa: E402
from src.datasources import grrr  # noqa: E402
from src.datasources.glofas import GF_STATIONS, get_blob_name  # noqa: E402
from src.utils.rp_calc import empirical_return_periods  # noqa: E402

# ── Config ───────────────────────────────────────────────────────────────────
WET_MONTHS = [8, 9, 10, 11]
EVAL_YEARS = list(range(2003, 2023))  # GloFAS reforecast evaluation period
RA_YEARS = list(range(1998, 2023))  # GloFAS reanalysis evaluation period
GRRR_RF_YEARS = list(range(2016, 2024))  # GRRR reforecast evaluation period
MAX_LEADTIME = 16
RP_LEVELS = [3, 4, 5]

STATIC = {
    "Adamawa": {
        "station": "Wuroboki",
        "lgas": [
            "Demsa",
            "Fufore",
            "Girei",
            "Lamurde",
            "Numan",
            "Yola North",
            "Yola South",
        ],
    },
    "Benue": {
        "station": "Makurdi",
        "lgas": ["Agatu", "Guma", "Gwer West", "Logo", "Makurdi"],
    },
}


# ── Workflow data loaders ──────────────────────────────────────────────────


def get_event_years(state: str) -> dict | None:
    """Load Floodscan flood year flags from workflow blob."""
    blob = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_floodscan_annual.parquet"
    )
    try:
        df = stratus.load_parquet_from_blob(blob)
        return {
            rp: sorted(df[df[f"flood_{rp}yr"] == 1]["year"].tolist())
            for rp in RP_LEVELS
        }
    except Exception as e:
        print(f"  Warning: floodscan annual not available for {state}: {e}")
        return None


def get_floodscan_data(
    state: str, cfg: dict, event_years: dict | None
) -> tuple[dict, dict]:
    """
    Compute fs_crossings and fs_peak
    from workflow floodscan_daily parquet.
    """
    blob_daily = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_floodscan_daily.parquet"
    )
    blob_annual = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_floodscan_annual.parquet"
    )

    try:
        df_daily = stratus.load_parquet_from_blob(blob_daily)
        df_daily["date"] = pd.to_datetime(df_daily["date"])
        df_daily["year"] = df_daily["date"].dt.year
        df_daily = df_daily.rename(columns={"mean_sfed": "sfed"})
    except Exception as e:
        print(f"  Warning: floodscan daily not available for {state}: {e}")
        return {}, {}

    analysis_years = range(
        cfg["analysis_start_year"], cfg["analysis_end_year"] + 1
    )
    d = df_daily[df_daily["year"].isin(analysis_years)]

    if not event_years:
        print("  Skipping Floodscan crossings — event years unavailable.")
        return {}, {}

    # Derive RP thresholds: min annual max SFED across event years
    try:
        df_annual = stratus.load_parquet_from_blob(blob_annual)
        ann_max = df_annual.set_index("year")["max_sfed"]
    except Exception:
        ann_max = d.groupby("year")["sfed"].max()

    rp_thresh = {}
    for rp in RP_LEVELS:
        ev = [y for y in event_years.get(rp, []) if y in ann_max.index]
        if ev:
            rp_thresh[rp] = float(ann_max.loc[ev].min())
    rounded = {k: round(v, 4) for k, v in rp_thresh.items()}
    print(f"  Floodscan RP thresholds: {rounded}")

    # fs_crossings: first date sfed >= threshold per (year, rp)
    fs_crossings: dict = {}
    for rp, thr in rp_thresh.items():
        for yr, grp in d[d["sfed"] >= thr].groupby("year"):
            yr_s = str(yr)
            if yr_s not in fs_crossings:
                fs_crossings[yr_s] = {}
            fs_crossings[yr_s][str(rp)] = (
                grp["date"].min().strftime("%Y-%m-%d")
            )

    # fs_peak: date of annual peak sfed
    fs_peak = {
        str(yr): grp.loc[grp["sfed"].idxmax(), "date"].strftime("%Y-%m-%d")
        for yr, grp in d.groupby("year")
    }

    return fs_crossings, fs_peak


def get_trigger_grid(state: str) -> list | None:
    """Load pre-computed trigger grid from workflow blob."""
    blob = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_trigger_grid.parquet"
    )
    try:
        df = stratus.load_parquet_from_blob(blob)

        def _clean(v):
            if isinstance(v, float):
                return None if pd.isna(v) else round(v, 3)
            if hasattr(v, "item"):
                return v.item()
            return v

        return [
            {k: _clean(v) for k, v in row.items()}
            for row in df.to_dict("records")
        ]
    except Exception as e:
        print(f"  Warning: trigger grid not available for {state}: {e}")
        return None


# ── GloFAS data loaders (used by Readiness Trigger section) ──────────────────


def get_glofas_rp_thresholds(station: str) -> dict | None:
    """Compute empirical Weibull RP thresholds for a GloFAS station."""
    try:
        blob = get_blob_name("processed", "reanalysis", station)
        df = stratus.load_parquet_from_blob(blob)
    except Exception as e:
        print(
            f"  Warning: GloFAS reanalysis not available for RP "
            f"thresholds ({station}): {e}"
        )
        return None

    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    dis_col = next(
        c for c in df.columns if c.lower() in ("dis24", "discharge")
    )

    df_wet = df[
        df["month"].isin(WET_MONTHS) & df["year"].between(1998, 2022)
    ].copy()
    df_ann = df_wet.groupby("year")[dis_col].max().reset_index()
    df_ann["date"] = pd.to_datetime(df_ann["year"].astype(str) + "-01-01")

    try:
        df_rp = empirical_return_periods(
            df_ann, date_col="date", val_col=dis_col, target_rps=RP_LEVELS
        )
        result = {
            int(row["return_period"]): round(float(row["value"]), 1)
            for _, row in df_rp.iterrows()
        }
        print(f"  GloFAS RP thresholds ({station}): {result}")
        return result
    except Exception as e:
        print(f"  Warning: RP calculation failed for {station}: {e}")
        return None


def get_reforecast_data(
    cfg: dict, rp_thresholds: dict | None
) -> tuple[dict | None, dict | None]:
    """Compute annual_cum_max and first_exceedance from GloFAS reforecast.

    Returns:
        annual_cum_max: {year: {leadtime: cummax_discharge}}
            (threshold-independent)
        first_exceedance: {rp: {year: {leadtime: first_date}}}
            (one entry per RP level)
    """
    if rp_thresholds is None:
        return None, None

    try:
        df = stratus.load_parquet_from_blob(cfg["glofas_reforecast_blob"])
    except Exception as e:
        print(f"  Warning: GloFAS reforecast not available: {e}")
        return None, None

    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df = df[
        df["month"].isin(WET_MONTHS)
        & df["year"].isin(EVAL_YEARS)
        & df["leadtime"].between(1, MAX_LEADTIME)
    ]

    ens = (
        df.groupby(["year", "time", "leadtime"])["dis24"]
        .mean()
        .reset_index()
        .rename(columns={"dis24": "discharge"})
    )

    lt_year_max = ens.groupby(["year", "leadtime"])["discharge"].max()
    annual_cum_max = {}
    for year in EVAL_YEARS:
        if year not in lt_year_max.index.get_level_values("year"):
            continue
        running = float("-inf")
        yr_cm = {}
        for lt in range(1, MAX_LEADTIME + 1):
            try:
                val = float(lt_year_max.loc[(year, lt)])
                running = max(running, val)
            except KeyError:
                pass
            if running != float("-inf"):
                yr_cm[str(lt)] = round(running, 1)
        if yr_cm:
            annual_cum_max[str(year)] = yr_cm

    first_exceedance: dict = {}
    for rp, thresh in rp_thresholds.items():
        exceed = ens[ens["discharge"] > thresh]
        rp_exceedance: dict = {}
        for (year, lt), grp in exceed.groupby(["year", "leadtime"]):
            yr, lt_s = str(year), str(int(lt))
            if yr not in rp_exceedance:
                rp_exceedance[yr] = {}
            rp_exceedance[yr][lt_s] = grp["time"].min().strftime("%Y-%m-%d")
        first_exceedance[str(rp)] = rp_exceedance

    return annual_cum_max, first_exceedance


def get_reanalysis_exceed(cfg: dict, rp_thresholds: dict | None) -> dict:
    """First date reanalysis discharge exceeded RP threshold, per (rp, year).

    Returns: {rp: {year: first_date_str}}
    """
    station = cfg["glofas_station"]
    if rp_thresholds is None:
        return {}

    try:
        blob = get_blob_name("processed", "reanalysis", station)
        df = stratus.load_parquet_from_blob(blob)
    except Exception as e:
        print(f"  Warning: GloFAS reanalysis not available: {e}")
        return {}

    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    dis_col = next(
        c for c in df.columns if c.lower() in ("dis24", "discharge")
    )
    df_wet = df[
        df["month"].isin(WET_MONTHS) & df["year"].isin(RA_YEARS)
    ].copy()

    result = {}
    for rp, thresh in rp_thresholds.items():
        exceed = df_wet[df_wet[dis_col] > thresh]
        result[str(rp)] = {
            str(int(yr)): grp["time"].min().strftime("%Y-%m-%d")
            for yr, grp in exceed.groupby("year")
        }
    return result


# ── GRRR action trigger (updated to use workflow blobs) ──────────────────────


def _first_exceed(df_wet, gauge_ids, thresholds_by_gid, rp_keys):
    result = {rp: {} for rp in rp_keys}
    for gid in gauge_ids:
        df_g = df_wet[df_wet["gauge_id"] == gid]
        thresh_map = thresholds_by_gid.get(gid, {})
        for rp in rp_keys:
            thresh = thresh_map.get(rp)
            if thresh is None:
                continue
            exceed = df_g[df_g["streamflow"] > thresh]
            yr_dates = {}
            for yr, grp in exceed.groupby("year"):
                yr_dates[str(int(yr))] = grp["date"].min().strftime("%Y-%m-%d")
            if yr_dates:
                result[rp][gid] = yr_dates
    return result


def _first_exceed_rf(df_rf_wet, gauge_ids, thresholds_by_gid, rp_keys):
    result = {rp: {} for rp in rp_keys}
    for gid in gauge_ids:
        df_g = df_rf_wet[df_rf_wet["gauge_id"] == gid]
        thresh_map = thresholds_by_gid.get(gid, {})
        for rp in rp_keys:
            thresh = thresh_map.get(rp)
            if thresh is None:
                continue
            exceed = df_g[df_g["streamflow"] > thresh]
            yr_dates = {}
            for yr, grp in exceed.groupby("year"):
                yr_dates[str(int(yr))] = (
                    grp["issue_date"].min().strftime("%Y-%m-%d")
                )
            if yr_dates:
                result[rp][gid] = yr_dates
    return result


def get_action_data(state: str, cfg: dict) -> dict | None:
    """Load selected gauges from workflow blob and compute trigger data."""
    GOOGLE_RPS = [2, 5, 7]
    EMPIRICAL_RPS = [2, 3, 4, 5, 6]

    blob = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_selected_gauges.parquet"
    )
    try:
        df_sel = stratus.load_parquet_from_blob(blob)
    except Exception as e:
        print(f"  Warning: selected gauges not found for {state}: {e}")
        return None

    gauge_ids = df_sel[df_sel["source"] == "grrr"]["gauge_id"].tolist()
    if not gauge_ids:
        print(f"  Warning: no GRRR gauges in selected set for {state}")
        return None

    analysis_years = range(
        cfg["analysis_start_year"], cfg["analysis_end_year"] + 1
    )

    ds_ra = grrr.load_reanalysis(gauge=gauge_ids)
    df_ra = grrr.process_reanalysis(ds_ra)
    df_ra["date"] = pd.to_datetime(df_ra["valid_time"]).dt.normalize()
    df_ra["year"] = df_ra["date"].dt.year
    df_ra["month"] = df_ra["date"].dt.month

    df_wet = df_ra[
        df_ra["month"].isin(WET_MONTHS) & df_ra["year"].isin(analysis_years)
    ].copy()

    df_annual = (
        df_wet.groupby(["gauge_id", "year"])["streamflow"].max().reset_index()
    )

    gauge_annual_max = {
        gid: {
            str(int(r["year"])): round(float(r["streamflow"]), 2)
            for _, r in grp.iterrows()
        }
        for gid, grp in df_annual.groupby("gauge_id")
    }

    # Google official RP thresholds
    ds_rp = grrr.load_return_periods(gauge=gauge_ids)
    df_rp_raw = ds_rp.to_dataframe().reset_index()
    rp_cols = [c for c in df_rp_raw.columns if c.startswith("return_period_")]
    df_rp_long = df_rp_raw.melt(
        id_vars=["gauge_id"],
        value_vars=rp_cols,
        var_name="rp_raw",
        value_name="threshold",
    )
    df_rp_long["rp"] = df_rp_long["rp_raw"].str.extract(r"(\d+)").astype(int)
    df_rp_long = df_rp_long[df_rp_long["rp"].isin(GOOGLE_RPS)]

    google_rp_thresholds = {
        gid: {
            str(int(r["rp"])): round(float(r["threshold"]), 2)
            for _, r in grp.iterrows()
        }
        for gid, grp in df_rp_long.groupby("gauge_id")
    }

    # Empirical (Weibull) RP thresholds
    empirical_rp_thresholds = {}
    for gauge_id in gauge_ids:
        df_g = df_annual[df_annual["gauge_id"] == gauge_id].copy()
        if len(df_g) < 5:
            continue
        df_g["date"] = pd.to_datetime(df_g["year"].astype(str) + "-01-01")
        try:
            df_emp = empirical_return_periods(
                df_g,
                date_col="date",
                val_col="streamflow",
                target_rps=EMPIRICAL_RPS,
            )
            empirical_rp_thresholds[gauge_id] = {
                str(int(r["return_period"])): round(float(r["value"]), 2)
                for _, r in df_emp.iterrows()
            }
        except Exception:
            pass

    google_keys = [str(r) for r in GOOGLE_RPS]
    empirical_keys = [str(r) for r in EMPIRICAL_RPS]
    gauge_first_exceed = {
        "google": _first_exceed(
            df_wet, gauge_ids, google_rp_thresholds, google_keys
        ),
        "empirical": _first_exceed(
            df_wet, gauge_ids, empirical_rp_thresholds, empirical_keys
        ),
    }

    eval_years = sorted(int(y) for y in df_annual["year"].unique())

    # GRRR reforecast
    rf_annual_max = {}
    rf_first_exceed = {"google": {}, "empirical": {}}
    rf_eval_years = []

    rf_frames = []
    for gid in gauge_ids:
        try:
            ds_g = grrr.load_reforecast(gauge=gid)
            df_g = grrr.process_reforecast(ds_g)
            df_g["gauge_id"] = gid
            rf_frames.append(df_g)
        except Exception as exc:
            print(f"    Warning: reforecast unavailable for {gid}: {exc}")

    if rf_frames:
        df_rf = pd.concat(rf_frames, ignore_index=True)
        df_rf["issue_time"] = pd.to_datetime(df_rf["issue_time"])
        df_rf["valid_time"] = pd.to_datetime(df_rf["valid_time"])
        df_rf["issue_date"] = df_rf["issue_time"].dt.normalize()
        df_rf["valid_year"] = df_rf["valid_time"].dt.year
        df_rf["valid_month"] = df_rf["valid_time"].dt.month

        df_rf_wet = df_rf[
            df_rf["valid_month"].isin(WET_MONTHS)
            & df_rf["valid_year"].isin(GRRR_RF_YEARS)
        ].copy()
        df_rf_wet["year"] = df_rf_wet["valid_year"]

        df_rf_ann = (
            df_rf_wet.groupby(["gauge_id", "year"])["streamflow"]
            .max()
            .reset_index()
        )
        rf_annual_max = {
            gid: {
                str(int(r["year"])): round(float(r["streamflow"]), 2)
                for _, r in grp.iterrows()
            }
            for gid, grp in df_rf_ann.groupby("gauge_id")
        }
        rf_first_exceed = {
            "google": _first_exceed_rf(
                df_rf_wet, gauge_ids, google_rp_thresholds, google_keys
            ),
            "empirical": _first_exceed_rf(
                df_rf_wet, gauge_ids, empirical_rp_thresholds, empirical_keys
            ),
        }
        rf_eval_years = sorted(
            y
            for y in GRRR_RF_YEARS
            if any(str(y) in rf_annual_max.get(gid, {}) for gid in gauge_ids)
        )

    return {
        "eval_years": eval_years,
        "rf_eval_years": rf_eval_years,
        "gauge_ids": gauge_ids,
        "gauge_annual_max": gauge_annual_max,
        "rf_annual_max": rf_annual_max,
        "google_rp_thresholds": google_rp_thresholds,
        "empirical_rp_thresholds": empirical_rp_thresholds,
        "gauge_first_exceed": gauge_first_exceed,
        "rf_first_exceed": rf_first_exceed,
    }


def get_gauge_data(state: str) -> dict | None:
    """Load all correlated gauges; flag selected ones. Uses workflow blobs."""
    blob_corr = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_gauge_correlations.parquet"
    )
    blob_sel = (
        f"{PROJECT_PREFIX}/processed/workflow/"
        f"{state.lower()}_selected_gauges.parquet"
    )

    try:
        df_corr = stratus.load_parquet_from_blob(blob_corr)
    except Exception as e:
        print(f"  Warning: gauge correlations not available for {state}: {e}")
        return None

    try:
        selected_ids = set(
            stratus.load_parquet_from_blob(blob_sel)["gauge_id"].tolist()
        )
    except Exception:
        selected_ids = set()

    def _parse_row(row, is_selected):
        rec = {}
        for k, v in row.items():
            if k == "quality_verified":
                rec[k] = bool(v) if pd.notna(v) else None
            elif k in ("best_r", "latitude", "longitude"):
                rec[k] = round(float(v), 4) if pd.notna(v) else None
            elif k == "best_lag":
                rec[k] = int(v) if pd.notna(v) else None
            else:
                rec[k] = v
        rec["top_10"] = is_selected
        return rec

    keep = [
        c
        for c in [
            "gauge_id",
            "latitude",
            "longitude",
            "best_r",
            "best_lag",
            "quality_verified",
        ]
        if c in df_corr.columns
    ]

    if "source" in df_corr.columns:
        df_grrr = df_corr[df_corr["source"] == "grrr"]
        df_gf = df_corr[df_corr["source"] == "glofas"]
    else:
        df_grrr = df_corr
        df_gf = pd.DataFrame()

    gauges = [
        _parse_row(row, row["gauge_id"] in selected_ids)
        for _, row in df_grrr[keep].iterrows()
    ]

    glofas = None
    if not df_gf.empty:
        row = df_gf.iloc[0]
        glofas = {
            "lat": round(float(row["latitude"]), 4)
            if pd.notna(row.get("latitude"))
            else None,
            "lon": round(float(row["longitude"]), 4)
            if pd.notna(row.get("longitude"))
            else None,
            "best_r": round(float(row["best_r"]), 4)
            if pd.notna(row.get("best_r"))
            else None,
            "best_lag": int(row["best_lag"])
            if pd.notna(row.get("best_lag"))
            else None,
        }

    return {"gauges": gauges, "glofas": glofas}


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    out = {}

    for state in ["Adamawa", "Benue"]:
        print(f"\n{state}")
        cfg = STATE_CONFIG[state]

        print("  event years...")
        event_years = get_event_years(state)

        print("  GloFAS RP thresholds...")
        glofas_rp_thresholds = get_glofas_rp_thresholds(cfg["glofas_station"])

        print("  GloFAS reforecast...")
        annual_cum_max, first_exceedance = get_reforecast_data(
            cfg, glofas_rp_thresholds
        )

        print("  GloFAS reanalysis...")
        reanalysis_exceed = get_reanalysis_exceed(cfg, glofas_rp_thresholds)

        print("  Floodscan...")
        fs_crossings, fs_peak = get_floodscan_data(state, cfg, event_years)

        print("  trigger grid...")
        trigger_grid = get_trigger_grid(state)

        print("  action trigger...")
        action = get_action_data(state, cfg)

        print("  gauge overview...")
        gauge_data = get_gauge_data(state)
        gauges = gauge_data["gauges"] if gauge_data else None
        glofas_coords = gauge_data["glofas"] if gauge_data else None
        if glofas_coords is None:
            glofas_coords = GF_STATIONS.get(cfg["glofas_station"])

        out[state] = {
            **STATIC[state],
            "thresh": cfg["glofas_thresh"],
            "glofas_rp_thresholds": {
                str(k): v for k, v in (glofas_rp_thresholds or {}).items()
            },
            "action_lt": cfg["glofas_leadtime_action"],
            "analysis_start_year": cfg["analysis_start_year"],
            "analysis_end_year": cfg["analysis_end_year"],
            "event_years": event_years,
            "reanalysis_exceed": reanalysis_exceed,
            "annual_cum_max": annual_cum_max,
            "first_exceedance": first_exceedance,
            "fs_crossings": fs_crossings,
            "fs_peak": fs_peak,
            "trigger_grid": trigger_grid,
            "action": action,
            "gauges": gauges,
            "glofas_coords": glofas_coords,
        }

    here = Path(__file__).parent
    json_path = here / "data.json"
    json_path.write_text(json.dumps(out, indent=2) + "\n")
    json_kb = round(json_path.stat().st_size / 1024)
    print(f"\nWrote {json_path}  ({json_kb} KB)")


if __name__ == "__main__":
    main()
