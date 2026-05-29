#!/usr/bin/env python3
"""
Generate dashboard data for the CERF Nigeria trigger explorer.

Reads from blob storage and writes two files:
  data.json  — raw data (source of truth, human-readable)
  data.js    — JS wrapper: const DASHBOARD_DATA = {...};

Usage (from repo root or this directory):
    python exploration/2026/cerf/web/generate_data.py

The data.js file is loaded by index.html via <script src="data.js">.

── Required blob inputs ──────────────────────────────────────────────────────

The following blobs must exist in Azure storage before running this script.
The notebook that produces each one is noted alongside.

  Trigger matrix  [notebook 08_trigger_events.ipynb]
    ds-aa-nga-flooding/processed/trigger_matrix/adamawa_trigger_matrix.parquet
    ds-aa-nga-flooding/processed/trigger_matrix/benue_trigger_matrix.parquet

  Floodscan state pixels  [notebook 01b_nihsa_floodrisk.ipynb or equivalent]
    ds-aa-nga-flooding/processed/floodscan/fs_adamawa_pixels_1998_2025.parquet
    ds-aa-nga-flooding/processed/floodscan/fs_benue_state_pixels_1998_2025.parquet

  GloFAS reanalysis  [download_glofas_reanalysis.ipynb]
    ds-aa-nga-flooding/processed/glofas/glofas_reanalysis_wuroboki.parquet
    ds-aa-nga-flooding/processed/glofas/glofas_reanalysis_makurdi.parquet

  GloFAS reforecast  [download_glofas_reforecast.ipynb]
    ds-aa-nga-flooding/processed/glofas/wuroboki_glofas_reforecast_ens.parquet
    ds-aa-nga-flooding/processed/glofas/glofas_reforecast_makurdi_ensemble.parquet

The trigger matrix is the primary dependency: Floodscan RP thresholds are
derived from it, and if it is unavailable for a state the Floodscan section
of the dashboard will be empty for that state.
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
from src.datasources.glofas import get_blob_name  # noqa: E402

# ── Config ───────────────────────────────────────────────────────────────────
WET_MONTHS = [8, 9, 10, 11]
EVAL_YEARS = list(range(2003, 2023))
MAX_LEADTIME = 16
RP_LEVELS = [3, 4, 5]

# Static metadata not derivable from data sources
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


# ── Data loaders ─────────────────────────────────────────────────────────────
def get_event_years(state: str) -> dict | None:
    """Load Floodscan flood year flags from the trigger matrix parquet."""
    blob = (
        f"{PROJECT_PREFIX}/processed/trigger_matrix"
        f"/{state.lower()}_trigger_matrix.parquet"
    )
    try:
        df = stratus.load_parquet_from_blob(blob)
        return {
            rp: sorted(df[df[f"fs_{rp}yr"] == 1]["year"].tolist())
            for rp in RP_LEVELS
        }
    except Exception as e:
        print(f"  Warning: trigger matrix not available for {state}: {e}")
        return None


def get_reforecast_data(cfg: dict) -> tuple[dict | None, dict | None]:
    """Compute annual_cum_max and first_exceedance from GloFAS reforecast.

    annual_cum_max[year][lt] = running max ensemble-mean discharge across all
    issue dates and leadtimes 1..lt during the wet season.

    first_exceedance[year][lt] = earliest issue date at exactly leadtime lt
    where ensemble mean exceeded the threshold.
    """
    thresh = cfg["glofas_thresh"]
    if thresh is None:
        return None, None

    df = stratus.load_parquet_from_blob(cfg["glofas_reforecast_blob"])
    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month

    df = df[
        df["month"].isin(WET_MONTHS)
        & df["year"].isin(EVAL_YEARS)
        & df["leadtime"].between(1, MAX_LEADTIME)
    ]

    # Ensemble mean per (year, time, leadtime)
    ens = (
        df.groupby(["year", "time", "leadtime"])["dis24"]
        .mean()
        .reset_index()
        .rename(columns={"dis24": "discharge"})
    )

    # annual_cum_max: running max as leadtime increases from 1 to MAX_LEADTIME
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

    # first_exceedance: first issue date per (year, leadtime) above thresh
    exceed = ens[ens["discharge"] > thresh]
    first_exceedance: dict = {}
    for (year, lt), grp in exceed.groupby(["year", "leadtime"]):
        yr, lt_s = str(year), str(int(lt))
        if yr not in first_exceedance:
            first_exceedance[yr] = {}
        first_exceedance[yr][lt_s] = grp["time"].min().strftime("%Y-%m-%d")

    return annual_cum_max, first_exceedance


def get_reanalysis_exceed(cfg: dict) -> dict:
    """First date reanalysis discharge exceeded thresh per year."""
    thresh = cfg["glofas_thresh"]
    station = cfg["glofas_station"]
    if thresh is None:
        return {}

    blob = get_blob_name("processed", "reanalysis", station)
    df = stratus.load_parquet_from_blob(blob)
    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month

    dis_col = next(
        c for c in df.columns if c.lower() in ("dis24", "discharge")
    )
    exceed = df[
        df["month"].isin(WET_MONTHS)
        & df["year"].isin(EVAL_YEARS)
        & (df[dis_col] > thresh)
    ]
    return {
        str(int(yr)): grp["time"].min().strftime("%Y-%m-%d")
        for yr, grp in exceed.groupby("year")
    }


def get_floodscan_data(
    cfg: dict, event_years: dict | None
) -> tuple[dict, dict]:
    """Compute fs_crossings and fs_peak from Floodscan state pixel parquet.

    RP thresholds are derived from the trigger matrix event years (the minimum
    annual max SFED across those years) so they are guaranteed to be consistent
    with notebook 08. Falls back to rank-based if event_years is unavailable.

    fs_crossings[year][rp] = first date daily mean SFED >= rp threshold.
    fs_peak[year]           = date of annual peak daily mean SFED.
    """
    df = stratus.load_parquet_from_blob(cfg["floodscan_blob"])
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year

    # Daily mean SFED across pixels
    daily = (
        df.groupby("date")["SFED"]
        .mean()
        .reset_index()
        .rename(columns={"SFED": "sfed", "date": "time"})
    )
    daily["year"] = daily["time"].dt.year

    analysis_years = range(
        cfg["analysis_start_year"], cfg["analysis_end_year"] + 1
    )
    d = daily[daily["year"].isin(analysis_years)]

    ann_max_by_year = d.groupby("year")["sfed"].max()

    # Derive RP thresholds from trigger matrix event years: threshold = minimum
    # annual max SFED across event years — matches notebook 08's cut-off.
    if not event_years:
        print("  Skipping Floodscan crossings — trigger matrix unavailable.")
        return {}, {}

    rp_thresh = {}
    for rp in RP_LEVELS:
        ev = [y for y in event_years.get(rp, []) if y in ann_max_by_year.index]
        if ev:
            rp_thresh[rp] = float(ann_max_by_year.loc[ev].min())
    thresholds = {k: round(v, 4) for k, v in rp_thresh.items()}
    print(f"  Floodscan RP thresholds (from trigger matrix): {thresholds}")

    # fs_crossings: first date SFED >= threshold per (year, rp)
    fs_crossings: dict = {}
    for rp, thr in rp_thresh.items():
        for yr, grp in d[d["sfed"] >= thr].groupby("year"):
            yr_s = str(yr)
            if yr_s not in fs_crossings:
                fs_crossings[yr_s] = {}
            fs_crossings[yr_s][str(rp)] = (
                grp["time"].min().strftime("%Y-%m-%d")
            )

    # fs_peak
    fs_peak = {
        str(yr): grp.loc[grp["sfed"].idxmax(), "time"].strftime("%Y-%m-%d")
        for yr, grp in d.groupby("year")
    }

    return fs_crossings, fs_peak


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    out = {}

    for state in ["Adamawa", "Benue"]:
        print(f"\n{state}")
        cfg = STATE_CONFIG[state]

        print("  event years...")
        event_years = get_event_years(state)

        print("  reforecast...")
        annual_cum_max, first_exceedance = get_reforecast_data(cfg)

        print("  reanalysis...")
        reanalysis_exceed = get_reanalysis_exceed(cfg)

        print("  Floodscan...")
        fs_crossings, fs_peak = get_floodscan_data(cfg, event_years)

        out[state] = {
            **STATIC[state],
            "thresh": cfg["glofas_thresh"],
            "action_lt": cfg["glofas_leadtime_action"],
            "analysis_start_year": cfg["analysis_start_year"],
            "analysis_end_year": cfg["analysis_end_year"],
            "event_years": event_years,
            "reanalysis_exceed": reanalysis_exceed,
            "annual_cum_max": annual_cum_max,
            "first_exceedance": first_exceedance,
            "fs_crossings": fs_crossings,
            "fs_peak": fs_peak,
        }

    here = Path(__file__).parent
    json_path = here / "data.json"
    js_path = here / "data.js"

    json_str = json.dumps(out, separators=(",", ":"))
    json_path.write_text(json_str + "\n")
    js_path.write_text("const DASHBOARD_DATA = " + json_str + ";\n")

    json_kb = round(json_path.stat().st_size / 1024)
    js_kb = round(js_path.stat().st_size / 1024)
    print(f"\nWrote {json_path}  ({json_kb} KB)")
    print(f"Wrote {js_path}  ({js_kb} KB)")


if __name__ == "__main__":
    main()
