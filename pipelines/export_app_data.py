"""Export the monitoring-app data bundle (static JSON for the GH Pages app).

Runs in CI at deploy time (see .github/workflows/deploy-app.yml): reads the
config registries + trigger results from blob, fetches the latest Google
forecasts, and writes JSON under app/data/ (gitignored — data never enters
git). The static app (app/index.html + app.js) is pure client-side.

Outputs:
    app/data/core.json   registries, trigger configs/performance, season
                         detail, reforecast skill, live forecasts
    app/data/geo.json    simplified state + riverine-LGA boundaries

    python pipelines/export_app_data.py            # writes app/data/
"""

import argparse
import json
import os
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")
# align cds_utils' download dir with etl.process_glofas's hardcoded "temp/"
os.environ.setdefault("CDS_TEMP_DIR", "temp")

import ocha_stratus as stratus  # noqa: E402

from src.config import (  # noqa: E402
    load_gauge_registry,
    load_lga_registry,
    load_state_params,
)
from src.constants import PROJECT_PREFIX  # noqa: E402
from src.datasources import glofas as glofas_ds  # noqa: E402
from src.datasources import grrr  # noqa: E402
from src.monitoring import etl  # noqa: E402

OUT_DIR = Path(__file__).parents[1] / "app" / "data"
CFG = PROJECT_PREFIX + "/processed/config/"


def _records(df, cols=None):
    """JSON-safe records: NaN -> None (NaN is invalid JSON)."""
    d = df[cols] if cols else df
    return json.loads(d.to_json(orient="records"))


def _load_blob(name):
    try:
        return stratus.load_parquet_from_blob(CFG + name)
    except Exception:
        return pd.DataFrame()


def fetch_live(gauge_ids):
    """Latest forecast horizon per GRRR gauge: peak + timeseries."""
    try:
        f = grrr.get_gauge_forecasts(list(gauge_ids))
    except Exception as e:
        print(f"  ! live forecast fetch failed: {e}", flush=True)
        return {}, None
    if f.empty:
        return {}, None
    f = f.sort_values("issued_time")
    latest = f.groupby("gauge_id")["issued_time"].transform("max")
    cur = f[f["issued_time"] == latest]
    out = {}
    for gid, sub in cur.groupby("gauge_id"):
        out[gid] = {
            "peak": float(sub["value"].max()),
            "ts": [{"t": t.isoformat(), "v": round(float(v), 1)}
                   for t, v in zip(sub["forecast_start"], sub["value"])],
        }
    return out, f["issued_time"].max().isoformat()


def _glofas_local(blob_name, coords, date):
    """Ensure the day's forecast GRIB is at temp/{blob_name}: reuse the local
    file, else pull the cached blob, else request from CDS (which uploads to
    blob AND keeps the local copy)."""
    path = Path("temp") / blob_name
    if path.exists():
        return
    bc = stratus.get_container_client("projects", "dev").get_blob_client(blob_name)
    if bc.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bc.download_blob().readall())
        return
    etl.get_glofas_forecast(blob_name, coords, date)


def fetch_live_glofas(stations):
    """Latest operational GloFAS forecast per station (5-day ens-mean),
    same shape as the Google live entries. Tries today, then back 2 days
    (the operational product lands with a delay)."""
    out, issued = {}, None
    for stn in stations:
        for lag in (0, 1, 2):
            d = datetime.now(timezone.utc).date() - timedelta(days=lag)
            date = pd.Timestamp(d)
            blob = etl.get_blob_name("forecast", stn, date)
            try:
                _glofas_local(blob, glofas_ds.get_coords(stn), date)
                df = etl.process_glofas(blob, "glofas_forecast", stn)
            except Exception as e:
                print(f"  ! glofas {stn} {d}: {type(e).__name__}: {e}",
                      flush=True)
                continue
            if df.empty:
                continue
            out[stn] = {
                "peak": float(df["value"].max()),
                "issued": str(d),
                "ts": [{"t": pd.Timestamp(t).isoformat(),
                        "v": round(float(v), 1)}
                       for t, v in zip(df["valid_date"], df["value"])],
            }
            issued = max(issued or str(d), str(d))
            break
    return out, issued


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-live", action="store_true",
                   help="skip the Google live-forecast fetch (offline test)")
    args = p.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading registries + results from blob…", flush=True)
    lga = load_lga_registry()
    gauges = load_gauge_registry()
    sp = load_state_params()
    perf = _load_blob("trigger_performance.parquet")
    year_detail = _load_blob("trigger_year_detail.parquet")
    skill = _load_blob("reforecast_skill.parquet")

    live, issued, gf_issued = {}, None, None
    if not args.skip_live:
        grrr_sel = gauges[(gauges["is_selected"] == True)  # noqa: E712
                          & (gauges["source"] == "grrr")]["gauge_id"].unique()
        print(f"Fetching live Google forecasts for {len(grrr_sel)} gauges…",
              flush=True)
        live, issued = fetch_live(grrr_sel)
        gf_sel = gauges[(gauges["is_selected"] == True)  # noqa: E712
                        & (gauges["source"] == "glofas")]["gauge_id"].unique()
        print(f"Fetching live GloFAS forecasts for {len(gf_sel)} stations…",
              flush=True)
        gf_live, gf_issued = fetch_live_glofas(sorted(gf_sel))
        live.update(gf_live)

    core = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "live_issued": issued,
        "glofas_issued": gf_issued,
        "states": _records(sp),
        "perf": _records(perf),
        "gauges": _records(gauges.drop(columns=["geometry"], errors="ignore")),
        "lgas": _records(lga),
        "year_detail": _records(year_detail),
        "skill": _records(skill),
        "live": live,
    }
    (OUT_DIR / "core.json").write_text(json.dumps(core))
    print(f"  core.json  {(OUT_DIR / 'core.json').stat().st_size / 1e6:.2f} MB",
          flush=True)

    print("Building simplified boundaries…", flush=True)
    adm1 = stratus.codab.load_codab_from_blob("NGA", admin_level=1)
    adm1 = adm1[adm1["ADM1_EN"].isin(set(sp["state"]))][
        ["ADM1_EN", "geometry"]].copy()
    adm1["geometry"] = adm1.geometry.simplify(0.01)
    adm2 = stratus.codab.load_codab_from_blob("NGA", admin_level=2)
    riverine = adm2[adm2["ADM2_PCODE"].isin(set(lga["pcode"]))][
        ["ADM2_PCODE", "ADM2_EN", "ADM1_EN", "geometry"]].copy()
    riverine["geometry"] = riverine.geometry.simplify(0.005)
    geo = {
        "states": json.loads(adm1.to_json()),
        "lgas": json.loads(riverine.to_json()),
    }
    (OUT_DIR / "geo.json").write_text(json.dumps(geo))
    print(f"  geo.json   {(OUT_DIR / 'geo.json').stat().st_size / 1e6:.2f} MB",
          flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
