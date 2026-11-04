"""Variant: re-select the 10 gauges on the new Google model, then apply the trigger.

The comparison page keeps the endorsed 10 gauges and refits only their levels.
This variant also re-runs the design's gauge selection on the new model,
exactly as in exploration/2026/cerf/workflow/:

  01  candidate pool: the 42 Google gauges whose catchment intersects the 7
      target LGAs (Flood Hub API, all quality levels) + the GloFAS Wuroboki
      station (workflow/adamawa_gauges.parquet, unchanged)
  03  each candidate's best Spearman correlation with FloodScan SFED,
      Aug-Nov 1998-2023, lags -3..+14 days (analysis.best_lag_corr)
  04  rank all candidates by that correlation, keep the top 10

then applies the endorsed rule (1-in-4 level per gauge, >= 6 of 10 on the
same day, leads 0-7) with analysis.py's functions. The GloFAS station keeps
its stored correlation (its series does not change with the Google model).
The old-model run must reproduce the stored correlations and the endorsed 10.

    PYTHONPATH=. .venv/bin/python exploration/2026/cerf/google-model-update/reselect.py
"""

import os
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import analysis as A  # noqa: E402

TOP_N = 10


def load_pool():
    import ocha_stratus as stratus

    gauges = stratus.load_parquet_from_blob("ds-aa-nga-flooding/processed/workflow/adamawa_gauges.parquet")
    corr = stratus.load_parquet_from_blob("ds-aa-nga-flooding/processed/workflow/adamawa_gauge_correlations.parquet")
    return gauges, corr


def rank(ra, fs_daily, pool, glofas_row):
    """03 + 04: correlations for every Google candidate, GloFAS row as stored, ranked."""
    rows = []
    for _, g in pool.iterrows():
        r, lag = A.best_lag_corr(ra, fs_daily, g.gauge_id)
        rows.append({"gauge_id": g.gauge_id, "source": "grrr", "best_r": r, "best_lag": lag,
                     "latitude": g.latitude, "longitude": g.longitude, "river": g.river,
                     "quality_verified": bool(g.quality_verified)})
    rows.append({"gauge_id": glofas_row.gauge_id, "source": "glofas", "best_r": float(glofas_row.best_r),
                 "best_lag": int(glofas_row.best_lag), "latitude": glofas_row.latitude,
                 "longitude": glofas_row.longitude, "river": "Benue", "quality_verified": None})
    df = pd.DataFrame(rows).sort_values("best_r", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df


def trigger(ra, rf, gauge_ids, fs_events):
    """The endorsed rule on one gauge set (analysis.py, unchanged)."""
    thr = A.thresholds(ra, gauge_ids)
    votes = A.reanalysis_votes(ra, thr)
    n_exc, years = A.reanalysis_years(votes)
    rf_complete, rf_n_exc, rf_years = A.reforecast_years(rf, thr)
    yrs = list(range(A.START, A.END + 1))
    return {"thr": thr, "n_exc": n_exc, "years": years, "first_crossing": A.first_crossing(votes),
            "rf_complete": rf_complete, "rf_n_exc": rf_n_exc, "rf_years": rf_years,
            "rf_first_forecast": A.first_forecast(rf, thr, rf_years),
            "scores": {rp: A.score(years, fs_events[rp], yrs) for rp in A.FS_RP_LEVELS},
            "margin": A.margin(ra, thr),
            "sweep": {rp: A.reanalysis_years(A.reanalysis_votes(ra, A.thresholds(ra, gauge_ids, rp=rp)))[1]
                      for rp in (3, 4, 5, 6)}}


def run():
    sel, fs_ann, fs_daily = A.load_inputs()
    gauges, corr = load_pool()
    pool = gauges[gauges.source == "grrr"].reset_index(drop=True)
    glofas_row = corr[corr.source == "glofas"].iloc[0]
    fs_events = {rp: set(fs_ann[fs_ann[f"flood_{rp}yr"] == 1]["year"]) for rp in A.FS_RP_LEVELS}
    ids = pool.gauge_id.tolist()
    out = {"pool": pool, "fs_events": fs_events, "endorsed": sel.gauge_id.tolist()}
    for v in ("old", "new"):
        ra, rf = A.fetch(v, ids, tag="_pool")
        ranked = rank(ra, fs_daily, pool, glofas_row)
        top = ranked.iloc[:TOP_N]
        assert (top.source == "grrr").all(), "a GloFAS station entered the top 10: the trigger would mix sources"
        out[v] = {"ra": ra, "rf": rf, "ranked": ranked, "selected": top.gauge_id.tolist()}
    # gate: the old model reproduces step 03's stored correlations and step 04's endorsed selection
    chk = out["old"]["ranked"].set_index("gauge_id").join(corr.set_index("gauge_id")[["best_r", "best_lag"]], rsuffix="_stored")
    chk = chk[chk.source == "grrr"]
    assert (chk.best_r - chk.best_r_stored).abs().max() < 1e-6, (chk.best_r - chk.best_r_stored).abs().max()
    assert (chk.best_lag == chk.best_lag_stored).all()
    assert out["old"]["selected"] == out["endorsed"], out["old"]["selected"]
    # the three configurations, all on the shared years
    out["configs"] = {
        "old_endorsed": trigger(out["old"]["ra"], out["old"]["rf"], out["endorsed"], fs_events),
        "new_endorsed": trigger(out["new"]["ra"], out["new"]["rf"], out["endorsed"], fs_events),
        "new_reselected": trigger(out["new"]["ra"], out["new"]["rf"], out["new"]["selected"], fs_events),
    }
    assert out["configs"]["old_endorsed"]["years"] == A.ENDORSED_YEARS
    return out


if __name__ == "__main__":
    res = run()
    for v in ("old", "new"):
        rk = res[v]["ranked"]
        print(f"\n=== {v}: top 15")
        print(rk.head(15)[["rank", "gauge_id", "source", "best_r", "best_lag", "latitude", "longitude", "river"]].round(3).to_string())
    new_sel, end = set(res["new"]["selected"]), set(res["endorsed"])
    print("\nkept", sorted(new_sel & end), "\nin", sorted(new_sel - end), "\nout", sorted(end - new_sel))
    for k, c in res["configs"].items():
        print(f"\n--- {k}: years {c['years']} rf {c['rf_years']} (complete {c['rf_complete']})")
        print("  levels", c["thr"].round(1).to_dict())
        print("  max gauges by year", {y: int(n) for y, n in c["n_exc"].items() if n})
        print("  first day", {y: str(d.date()) for y, d in c["first_crossing"].items()})
        print("  first forecast", {y: (str(a.date()), lt) for y, (a, b, lt) in c["rf_first_forecast"].items()})
        print("  sweep", c["sweep"])
    sys.stdout.flush()
    os._exit(0)
