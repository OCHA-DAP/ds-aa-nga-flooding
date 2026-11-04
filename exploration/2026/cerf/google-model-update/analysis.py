"""Old vs new Google model (GRRR) under the endorsed 2026 Adamawa action trigger.

Google released a new version of its Runoff Reanalysis & Reforecast (GRRR) in
November 2026 ("distributed_model_2026_11", a semi-distributed model on the
HydroBASINS river graph). The 2026 action trigger was designed on the earlier
version ("model_id_8583a5c2_v0"). This module replays the trigger design on
both versions with the method held fixed, exactly as in
exploration/2026/cerf/workflow/06_trigger_definition.ipynb:

  - the same 10 gauges (workflow/adamawa_selected_gauges.parquet)
  - wet season Aug-Nov, calibration years 1998-2023
  - per-gauge 1-in-4 level: empirical Weibull plotting position, log-log
    interpolation, on wet-season annual maxima of the model's own reanalysis
  - a year activates when >= 6 of 10 gauges are strictly above their own
    level on the same day
  - reforecast: valid_time = issue_time + lead_time, leads 0-7, best value per
    (valid day, gauge) across issues, years with reforecast through October

Only the per-gauge levels are refitted. Everything else is the endorsed rule.

    PYTHONPATH=. .venv/bin/python exploration/2026/cerf/google-model-update/analysis.py
"""

import os
import sys
from pathlib import Path

os.environ.setdefault("GRPC_VERBOSITY", "NONE")

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))

CACHE = REPO / "temp" / "google-model-update"
STORES = {
    "old": "gs://flood-forecasting/hydrologic_predictions/model_id_8583a5c2_v0",
    "new": "gs://flood-forecasting/hydrologic_predictions/distributed_model_2026_11",
}
PREFIX = {"old": "hybas_", "new": "subhybas_"}

RP_THRESH = 4
N_GAUGES = 6
MAX_LEAD_TIME = 7
WET_MONTHS = [8, 9, 10, 11]
FS_RP_LEVELS = [3, 4, 5]
START, END = 1998, 2023  # STATE_CONFIG["Adamawa"] analysis years
MIN_LAG, MAX_LAG, MIN_OBS = -3, 14, 60  # workflow/03_forecast_correlation.ipynb

# The endorsed per-gauge levels (src/constants.py ACTION_GAUGE_THRESHOLDS on
# main, framework document annex). The old-model replay must reproduce them.
ENDORSED = {
    "hybas_1120842990": 1110.8, "hybas_1120843610": 1101.5,
    "hybas_1120845060": 1102.2, "hybas_1120849600": 1113.7,
    "hybas_1120848550": 1106.0, "hybas_1121970280": 1110.1,
    "hybas_1120842550": 1114.0, "hybas_1120840700": 1112.6,
    "hybas_1120840560": 1117.1, "hybas_1120840690": 142.7,
}
ENDORSED_YEARS = [1998, 1999, 2012, 2018, 2019, 2022]
ENDORSED_RF_YEARS = [2018, 2019, 2022]


# --------------------------------------------------------------- estimator
def empirical_return_periods(df, date_col, val_col, target_rps=None):
    """Verbatim from src/utils/rp_calc.py at commit 32c19fb (govt-2026-prep),
    the estimator 06_trigger_definition.ipynb imports."""
    if target_rps is None:
        target_rps = [2, 3, 5, 7, 10]

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["year"] = df[date_col].dt.year
    df_annual_max = df.groupby("year")[val_col].max().reset_index()

    vals = np.sort(df_annual_max[val_col].values.astype(float))[::-1]
    n = len(vals)
    emp_rps = (n + 1) / np.arange(1, n + 1)

    sort_idx = np.argsort(emp_rps)
    log_rps = np.log(emp_rps[sort_idx])
    log_vals = np.log(vals[sort_idx])

    thresholds = np.exp(np.interp(np.log(target_rps), log_rps, log_vals))
    return pd.DataFrame({"return_period": target_rps, "value": thresholds})


# ------------------------------------------------------------------- data
def _open(url):
    import zarr

    return zarr.open_group(url, mode="r", storage_options={"token": "anon"})


def _decode_time(arr):
    unit, origin = arr.attrs["units"].split(" since ")
    return pd.to_datetime(arr[:], unit={"days": "D"}[unit.strip()],
                          origin=pd.Timestamp(origin.strip()))


def _index(group, ids):
    want, out = set(ids), {}
    for pos, gid in enumerate(group["gauge_id"][:]):
        if str(gid) in want:
            out[str(gid)] = pos
    missing = [g for g in ids if g not in out]
    if missing:
        raise KeyError(f"not in store: {missing}")
    return out


def fetch(version, gauge_ids):
    """Reanalysis + reforecast for the gauges, cached under temp/.

    Rows carry the old-style gauge id (hybas_...) for both versions: the new
    store names the same HydroBASINS sub-basins subhybas_..., at identical
    outlet coordinates.
    """
    CACHE.mkdir(parents=True, exist_ok=True)
    ra_path, rf_path = CACHE / f"{version}_reanalysis.parquet", CACHE / f"{version}_reforecast.parquet"
    if ra_path.exists() and rf_path.exists():
        return pd.read_parquet(ra_path), pd.read_parquet(rf_path)
    store_ids = {g: PREFIX[version] + g.removeprefix("hybas_") for g in gauge_ids}

    g = _open(f"{STORES[version]}/reanalysis/streamflow.zarr")
    time, idx = _decode_time(g["time"]), _index(g, list(store_ids.values()))
    ra = pd.concat([pd.DataFrame({"gauge_id": gid, "valid_time": time,
                                  "streamflow": g["streamflow"][idx[sid], :]})
                    for gid, sid in store_ids.items()], ignore_index=True)

    g = _open(f"{STORES[version]}/reforecast/streamflow.zarr")
    issue, idx = _decode_time(g["issue_time"]), _index(g, list(store_ids.values()))
    leads = np.asarray(g["lead_time"][:]).astype(int)
    if g["lead_time"].attrs.get("units") != "days":
        leads = (pd.to_timedelta(g["lead_time"][:]) / pd.Timedelta("1D")).astype(int)
    frames = []
    for gid, sid in store_ids.items():
        arr = g["streamflow"][idx[sid], :, :]
        df = (pd.DataFrame(arr, index=pd.Index(issue, name="issue_time"),
                           columns=pd.Index(leads, name="leadtime"))
              .stack(future_stack=True).rename("streamflow").reset_index())
        df["gauge_id"] = gid
        frames.append(df)
    rf = pd.concat(frames, ignore_index=True)
    ra.to_parquet(ra_path)
    rf.to_parquet(rf_path)
    return ra, rf


def load_inputs():
    import ocha_stratus as stratus

    sel = stratus.load_parquet_from_blob("ds-aa-nga-flooding/processed/workflow/adamawa_selected_gauges.parquet")
    fs_ann = stratus.load_parquet_from_blob("ds-aa-nga-flooding/processed/workflow/adamawa_floodscan_annual.parquet")
    fs_daily = stratus.load_parquet_from_blob("ds-aa-nga-flooding/processed/workflow/adamawa_floodscan_daily.parquet")
    return sel, fs_ann, fs_daily


# --------------------------------------------------------------- analysis
def thresholds(ra, gauge_ids, rp=RP_THRESH, start=START, end=END):
    """06_trigger_definition.ipynb cell 7: per-gauge level at `rp`."""
    ra = ra.copy()
    ra["date"] = pd.to_datetime(ra["valid_time"]).dt.normalize()
    ra["year"], ra["month"] = ra["date"].dt.year, ra["date"].dt.month
    wet = ra[ra["month"].isin(WET_MONTHS) & ra["year"].between(start, end)]
    annual = wet.groupby(["gauge_id", "year"])["streamflow"].max().reset_index()
    out = {}
    for gid in gauge_ids:
        df_g = annual[annual["gauge_id"] == gid].copy()
        if len(df_g) < 5:
            continue
        df_g["date"] = pd.to_datetime(df_g["year"].astype(str) + "-01-01")
        df_rp = empirical_return_periods(df_g, date_col="date", val_col="streamflow", target_rps=[rp])
        out[gid] = df_rp.set_index("return_period")["value"][rp]
    return pd.Series(out)


def reanalysis_votes(ra, thr, start=START, end=END):
    """Daily count of gauges strictly above their level, wet season."""
    ra = ra.copy()
    ra["date"] = pd.to_datetime(ra["valid_time"]).dt.normalize()
    ra["year"], ra["month"] = ra["date"].dt.year, ra["date"].dt.month
    wet = ra[ra["month"].isin(WET_MONTHS) & ra["year"].between(start, end)
             & ra["gauge_id"].isin(thr.index)].copy()
    wet = wet.join(thr.rename("threshold"), on="gauge_id")
    wet["exceeded"] = (wet["streamflow"] > wet["threshold"]).astype(int)
    return wet.groupby(["year", "date"])["exceeded"].sum()


def reanalysis_years(votes, start=START, end=END):
    n_exc = votes.groupby("year").max().reindex(range(start, end + 1), fill_value=0)
    return n_exc, sorted(n_exc[n_exc >= N_GAUGES].index.tolist())


def first_crossing(votes):
    hit = votes[votes >= N_GAUGES].reset_index()
    return {int(y): g["date"].min() for y, g in hit.groupby("year")}


def _rf_frame(rf):
    rf = rf.copy()
    rf["valid_time"] = (pd.to_datetime(rf["issue_time"])
                        + pd.to_timedelta(rf["leadtime"], unit="D")).dt.normalize()
    rf["year"], rf["month"] = rf["valid_time"].dt.year, rf["valid_time"].dt.month
    return rf


def reforecast_years(rf, thr, max_year=END):
    """06_trigger_definition.ipynb cell 9: complete years, best value per day."""
    rf = _rf_frame(rf)
    complete = rf.groupby("year")["month"].max().pipe(lambda s: s[s >= 10].index.tolist())
    complete = [y for y in complete if START <= y <= max_year]
    wet = rf[rf["month"].isin(WET_MONTHS) & rf["leadtime"].le(MAX_LEAD_TIME) & rf["year"].isin(complete)]
    best = wet.groupby(["valid_time", "gauge_id", "year"])["streamflow"].max().reset_index()
    chk = best[best["gauge_id"].isin(thr.index)].join(thr.rename("threshold"), on="gauge_id")
    chk["exceeded"] = (chk["streamflow"] > chk["threshold"]).astype(int)
    n_exc = (chk.groupby(["year", "valid_time"])["exceeded"].sum()
             .groupby("year").max().reindex(complete, fill_value=0))
    return complete, n_exc, sorted(n_exc[n_exc >= N_GAUGES].index.tolist())


def first_forecast(rf, thr, years):
    """Year -> (first issue date, valid day, lead) on which one forecast put
    >= 6 gauges over their level on the same valid day (leads 0-7)."""
    rf = _rf_frame(rf)
    rf = rf[rf["month"].isin(WET_MONTHS) & rf["leadtime"].le(MAX_LEAD_TIME)
            & rf["gauge_id"].isin(thr.index) & rf["year"].isin(years)]
    rf = rf.join(thr.rename("threshold"), on="gauge_id")
    rf["exceeded"] = (rf["streamflow"] > rf["threshold"]).astype(int)
    v = rf.groupby(["issue_time", "valid_time", "leadtime", "year"])["exceeded"].sum().reset_index()
    hit = v[v["exceeded"] >= N_GAUGES].sort_values(["issue_time", "valid_time"])
    return {int(y): (g.iloc[0]["issue_time"], g.iloc[0]["valid_time"], int(g.iloc[0]["leadtime"]))
            for y, g in hit.groupby("year")}


def score(triggered_years, event_years, eval_years):
    """06_trigger_definition.ipynb cell 11 (TP/FP/FN, POD, FAR)."""
    trig, events = set(triggered_years) & set(eval_years), set(event_years) & set(eval_years)
    tp, fp = len(trig & events), len(trig - events)
    fn = len(events) - tp
    pod = tp / (tp + fn) if tp + fn else np.nan
    prec = tp / (tp + fp) if tp + fp else np.nan
    far = fp / (tp + fp) if tp + fp else np.nan
    f1 = 2 * pod * prec / (pod + prec) if (pod and prec and pod + prec > 0) else np.nan
    return {"TP": tp, "FP": fp, "FN": fn, "POD": pod, "FAR": far, "F1": f1}


def best_lag_corr(ra, fs_daily, gid, start=START, end=END):
    """03_forecast_correlation.ipynb: best Spearman vs FloodScan SFED, lags -3..14."""
    from scipy import stats as scipy_stats

    fs = fs_daily.copy()
    fs["date"] = pd.to_datetime(fs["date"])
    fs = fs[fs["date"].dt.month.isin(WET_MONTHS) & fs["date"].dt.year.between(start, end)]
    fs = fs.set_index("date").rename(columns={"mean_sfed": "sfed"})
    s = ra[ra.gauge_id == gid].copy()
    s["date"] = pd.to_datetime(s["valid_time"]).dt.normalize()
    merged = s.set_index("date")[["streamflow"]].join(fs[["sfed"]], how="inner")
    best_r, best_lag = 0.0, 0
    for lag in range(MIN_LAG, MAX_LAG + 1):
        sh = merged["sfed"].shift(-lag)
        ok = merged["streamflow"].notna() & sh.notna()
        if ok.sum() < MIN_OBS:
            continue
        r, _ = scipy_stats.spearmanr(merged["streamflow"][ok], sh[ok])
        if abs(r) > abs(best_r):
            best_r, best_lag = r, lag
    return float(best_r), int(best_lag)


def floodscan_reference(fs_daily, rp=RP_THRESH, start=START, end=END):
    """FloodScan SFED as a share of its own wet-season 1-in-4 level, and the
    first day each year it reaches that level: the timing reference used in
    workflow/exploration_peak_timing.ipynb (trigger day vs first FloodScan day
    over its 4-yr level)."""
    fs = fs_daily.rename(columns={"mean_sfed": "sfed"}).copy()
    fs["date"] = pd.to_datetime(fs["date"])
    fs = fs[fs["date"].dt.month.isin(WET_MONTHS)]
    fit = fs[fs["date"].dt.year.between(start, end)].copy()
    fit["date_col"] = fit["date"]
    level = float(empirical_return_periods(fit, "date_col", "sfed", [rp])["value"].iloc[0])
    ratio = fs.set_index("date")["sfed"] / level
    over = ratio[ratio >= 1]
    first = {int(y): g.index.min() for y, g in over.groupby(over.index.year)}
    return level, ratio, first


def margin(ra, thr, n=N_GAUGES):
    """Daily flow of the n-th highest gauge as a share of its own level, wet
    season, all years: the rule is met on days this is above 1."""
    ra = ra.copy()
    ra["date"] = pd.to_datetime(ra["valid_time"]).dt.normalize()
    ra = ra[ra["date"].dt.month.isin(WET_MONTHS) & ra["gauge_id"].isin(thr.index)]
    wide = ra.pivot_table(index="date", columns="gauge_id", values="streamflow")
    ratio = wide[thr.index] / thr
    return pd.Series(np.sort(ratio.values, axis=1)[:, ::-1][:, n - 1], index=ratio.index)


def weibull_rp(k, n):
    return (n + 1) / k if k else np.nan


def run():
    sel, fs_ann, fs_daily = load_inputs()
    gauge_ids = sel[sel["source"] == "grrr"]["gauge_id"].tolist()
    fs_events = {rp: set(fs_ann[fs_ann[f"flood_{rp}yr"] == 1]["year"]) for rp in FS_RP_LEVELS}
    years = list(range(START, END + 1))
    fs_level, fs_ratio, fs_first = floodscan_reference(fs_daily)
    res = {"gauges": sel, "fs_events": fs_events, "fs_level": fs_level, "fs_ratio": fs_ratio,
           "fs_first": fs_first, "fs_range": (fs_ratio.index.min(), fs_ratio.index.max())}
    for v in ("old", "new"):
        ra, rf = fetch(v, gauge_ids)
        thr = thresholds(ra, gauge_ids)
        votes = reanalysis_votes(ra, thr)
        n_exc, ra_years = reanalysis_years(votes)
        rf_complete, rf_n_exc, rf_years = reforecast_years(rf, thr)
        r = {
            "ra": ra, "rf": rf, "thr": thr, "votes": votes, "n_exc": n_exc, "years": ra_years,
            "first_crossing": first_crossing(votes),
            "rf_complete": rf_complete, "rf_n_exc": rf_n_exc, "rf_years": rf_years,
            "first_forecast": first_forecast(rf, thr, ra_years),
            "scores": {rp: score(ra_years, fs_events[rp], years) for rp in FS_RP_LEVELS},
            "rf_scores": {rp: score(rf_years, fs_events[rp], rf_complete) for rp in FS_RP_LEVELS},
            "corr": {g: best_lag_corr(ra, fs_daily, g) for g in gauge_ids},
            "ra_range": (pd.to_datetime(ra.valid_time).min(), pd.to_datetime(ra.valid_time).max()),
            "rf_range": (pd.to_datetime(rf.issue_time).min(), pd.to_datetime(rf.issue_time).max(),
                         int(rf.leadtime.max())),
            "sweep": {rp: reanalysis_years(reanalysis_votes(ra, thresholds(ra, gauge_ids, rp=rp)))[1]
                      for rp in (3, 4, 5, 6)},
        }
        r["margin"] = margin(ra, thr)
        r["rf_first_forecast"] = first_forecast(rf, thr, r["rf_years"])
        if v == "new":
            # what the live trigger would do if Google switched the API to the
            # new model before the frozen (old) levels were replaced
            old_thr = res["old"]["thr"]
            r["mismatch_years"] = reanalysis_years(reanalysis_votes(ra, old_thr, end=2025), end=2025)[1]
            # beyond the calibration record: levels stay fitted on 1998-2023
            votes_ext = reanalysis_votes(ra, thr, start=END + 1, end=2025)
            r["ext_n_exc"], r["ext_years"] = reanalysis_years(votes_ext, start=END + 1, end=2025)
            r["ext_first_crossing"] = first_crossing(votes_ext)
            r["ext_rf_complete"], r["ext_rf_n_exc"], r["ext_rf_years"] = reforecast_years(rf, thr, max_year=2025)
            r["ext_first_forecast"] = first_forecast(rf, thr, [2023, 2024, 2025])
        res[v] = r
    # the old replay must reproduce the endorsed design before any new number is trusted
    old = res["old"]
    diff = (old["thr"].round(1) - pd.Series(ENDORSED)).abs().max()
    assert diff < 0.05, f"old levels differ from the endorsed ones by {diff}"
    assert old["years"] == ENDORSED_YEARS, old["years"]
    assert old["rf_years"] == ENDORSED_RF_YEARS, old["rf_years"]
    return res


if __name__ == "__main__":
    res = run()
    print("FloodScan level", round(res["fs_level"], 4), "range", res["fs_range"])
    print("FloodScan first day over 1-in-4:", {y: str(d.date()) for y, d in res["fs_first"].items()})
    for v in ("old", "new"):
        r = res[v]
        print(f"\n=== {v}: RA {r['ra_range']}, RF {r['rf_range']}")
        print("levels:", r["thr"].round(1).to_dict())
        print("years:", r["years"], "RP", round(weibull_rp(len(r["years"]), END - START + 1), 1))
        print("max simultaneous by year:", r["n_exc"].to_dict())
        print("first crossing:", {y: str(d.date()) for y, d in r["first_crossing"].items()})
        print("first forecast:", {y: (str(a.date()), str(b.date()), lt) for y, (a, b, lt) in r["first_forecast"].items()})
        print("rf complete", r["rf_complete"], "rf years", r["rf_years"], dict(r["rf_n_exc"]))
        print("scores:", {k: {kk: (round(vv, 2) if isinstance(vv, float) else vv) for kk, vv in s.items()} for k, s in r["scores"].items()})
        print("corr:", {g[-5:]: (round(a, 3), b) for g, (a, b) in r["corr"].items()})
        print("sweep:", r["sweep"])
        print("rf first forecast:", {y: (str(a.date()), str(b.date()), lt) for y, (a, b, lt) in r["rf_first_forecast"].items()})
        print("margin peak by year:", r["margin"].groupby(r["margin"].index.year).max().round(2).loc[1998:].to_dict())
        if v == "new":
            print("MISMATCH old levels on new flows 1998-2025:", r["mismatch_years"])
            print("ext years 2024-25:", r["ext_years"], dict(r["ext_n_exc"]),
                  {y: str(d.date()) for y, d in r["ext_first_crossing"].items()})
            print("ext rf:", r["ext_rf_complete"], r["ext_rf_years"], dict(r["ext_rf_n_exc"]))
            print("ext first forecast:", {y: (str(a.date()), str(b.date()), lt) for y, (a, b, lt) in r["ext_first_forecast"].items()})
    sys.stdout.flush()
    os._exit(0)
