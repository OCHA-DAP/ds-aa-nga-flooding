"""Phase 2 — derive the per-state action trigger and fill the gauge registry.

For each state, reproduces the canonical workflow (methodology.md steps 3-6) on
Google GRRR reanalysis vs the state's Floodscan benchmark:

  3. rank each gauge by best lag-adjusted Spearman ρ (RAW daily streamflow vs
     daily-mean SFED, lag -3..+14 — per the canonical method, not anomalies)
  4. select the top-N gauges (default 10)
  5-6. per-gauge Weibull RP threshold at the target RP; evaluate the
     ≥consensus-fraction-of-N action trigger against Floodscan event years.

Monitoring is year-round, so annual maxima / correlation use all months.

Writes the enriched gauge registry (best_r, best_lag, rp_threshold, is_selected,
lead_time filled for GRRR gauges) and a per-state trigger-performance table.
GloFAS gauges are left null here (readiness trigger = Phase 3).

    python pipelines/build_trigger_config.py --states Adamawa   # validate one
    python pipelines/build_trigger_config.py                    # all states
"""

import argparse
import warnings

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import spearmanr

load_dotenv()
warnings.filterwarnings("ignore")

import ocha_stratus as stratus  # noqa: E402

from src.config import load_gauge_registry, load_state_params  # noqa: E402
from src.config.registry import GAUGE_REGISTRY_BLOB  # noqa: E402
from src.constants import PROJECT_PREFIX  # noqa: E402
from src.datasources import grrr  # noqa: E402

LAG_MIN, LAG_MAX = -3, 14   # canonical lag range (gauge leads = positive)
TOP_N = 10
MIN_OBS = 60
GAUGE_SOURCES = ("grrr", "glofas")
GF_REFORECAST_BLOB_FMT = (PROJECT_PREFIX + "/processed/glofas/"
                          "glofas_reforecast_{station}_ensemble.parquet")
# Flood seasons run Apr→Mar country-wide, labelled by start year (Apr 2022–Mar
# 2023 → season 2022). The Mar–Apr trough is the annual low everywhere (worst
# state sits at 4% of its climatological peak on Apr 1), so a Dec–Feb
# "black-flood" peak on the Niger stays attached to the season that produced it
# instead of being split into the next calendar year.
SEASON_START_MONTH = 4
FS_BLOB_FMT = PROJECT_PREFIX + "/processed/floodscan/fs_{state}_pixels_1998_2025.parquet"
PERF_BLOB = PROJECT_PREFIX + "/processed/config/trigger_performance.parquet"


def _season_year(dates) -> pd.Index:
    """Flood-season label for each date: the season's start calendar year."""
    d = pd.DatetimeIndex(dates)
    return pd.Index(d.year - (d.month < SEASON_START_MONTH).astype(int))


def _fs_daily(state):
    df = stratus.load_parquet_from_blob(
        FS_BLOB_FMT.format(state=state.lower().replace(" ", "_")))
    df["date"] = pd.to_datetime(df["date"])
    # drop the partial first season (record starts 1998-01-12, i.e. mid-season
    # 1997 with only the Jan–Mar trough — it would rank as a spurious low year)
    df = df[df["date"] >= f"1998-{SEASON_START_MONTH:02d}-01"]
    return df.groupby("date")["SFED"].mean().rename("sfed").reset_index()


def _annual_max(df, value, date="date"):
    """Max per flood season (Apr–Mar), keyed by season start year."""
    y = df.copy()
    y["year"] = _season_year(y[date])
    return y.groupby("year")[value].max()


_GF_CACHE = {}


def _glofas_daily(station):
    """Daily forecast proxy for a GloFAS station: ensemble-median dis24 at the
    shortest available lead per valid day. Loads the country-derived reforecast
    parquet directly (not load_glofas_reforecast, whose STATE_CONFIG override
    would give wuroboki/makurdi older, differently-scoped files). The archive
    holds Jun–Dec issues only (rainy-season download), so seasonal maxima come
    from that window — Jan–Feb black-flood tails are not observable here."""
    if station not in _GF_CACHE:
        df = stratus.load_parquet_from_blob(
            GF_REFORECAST_BLOB_FMT.format(station=station))
        med = df.groupby(["valid_time", "leadtime"])["dis24"].median().reset_index()
        med = med.sort_values("leadtime").drop_duplicates("valid_time")
        med["date"] = pd.to_datetime(med["valid_time"]).dt.normalize()
        _GF_CACHE[station] = (med.rename(columns={"dis24": "streamflow"})
                              [["date", "streamflow"]].sort_values("date")
                              .reset_index(drop=True))
    return _GF_CACHE[station].copy()


def candidate_gauges(state, gauge_reg):
    """Candidate rows for a state's trigger (GRRR + GloFAS). Adamawa stays
    pinned to the endorsed framework: in-state Google gauges only — it anchors
    the target activation frequency for all other states."""
    sub = gauge_reg[(gauge_reg["state"] == state)
                    & (gauge_reg["source"].isin(GAUGE_SOURCES))]
    if state == "Adamawa":
        sub = sub[sub["source"] == "grrr"]
        if "in_state" in sub.columns:
            sub = sub[sub["in_state"].fillna(True)]
    return sub


def load_gauge_daily(sub):
    """Long daily series (date, gauge_id, streamflow) for registry rows `sub`,
    mixing GRRR reanalysis and GloFAS reforecast daily proxies."""
    parts = []
    grrr_ids = sub[sub["source"] == "grrr"]["gauge_id"].tolist()
    if grrr_ids:
        ra = grrr.process_reanalysis(grrr.load_reanalysis(gauge=grrr_ids))
        ra["date"] = pd.to_datetime(ra["valid_time"]).dt.normalize()
        parts.append(ra[["date", "gauge_id", "streamflow"]])
    for station in sub[sub["source"] == "glofas"]["gauge_id"]:
        try:
            g = _glofas_daily(station)
        except Exception as e:
            print(f"  ! no GloFAS reforecast for {station}: {e}", flush=True)
            continue
        g["gauge_id"] = station
        parts.append(g[["date", "gauge_id", "streamflow"]])
    if not parts:
        return pd.DataFrame(columns=["date", "gauge_id", "streamflow"])
    return pd.concat(parts, ignore_index=True)


def _weibull_rp(annual_max):
    """Empirical Weibull RP per year: RP = (n+1)/rank (rank 1 = largest)."""
    s = annual_max.dropna().sort_values(ascending=False)
    n = len(s)
    rank = np.arange(1, n + 1)
    return pd.Series((n + 1) / rank, index=s.index)  # year -> RP


def _threshold_at_rp(annual_max, rp_target):
    """Interpolate the value at a target return period."""
    s = annual_max.dropna().sort_values(ascending=False)
    rp = (len(s) + 1) / np.arange(1, len(s) + 1)
    # RP decreases as value decreases; interpolate value vs RP
    return float(np.interp(rp_target, rp[::-1], s.values[::-1]))


def _event_years(sfed_annual_max, rp_target):
    rp = _weibull_rp(sfed_annual_max)
    return set(rp[rp >= rp_target].index)


def process_state(state, cfg, gauge_reg):
    fs = _fs_daily(state)
    fs_years = _annual_max(fs, "sfed")
    events = _event_years(fs_years, cfg["rp_target"])

    sub = candidate_gauges(state, gauge_reg)
    gauges = sub["gauge_id"].tolist()
    if not gauges:
        return None, None
    ra = load_gauge_daily(sub)

    y0, y1 = cfg["analysis_start_year"], cfg["analysis_end_year"]
    fs_sy = _season_year(fs["date"])
    fs_w = fs[(fs_sy >= y0) & (fs_sy <= y1)]

    rows, daily_exceed = [], {}
    for gid in gauges:
        g = ra[ra["gauge_id"] == gid][["date", "streamflow"]].dropna()
        g_sy = _season_year(g["date"])
        g = g[(g_sy >= y0) & (g_sy <= y1)]
        m = g.merge(fs_w, on="date")
        if len(m) < MIN_OBS:
            continue
        # best lag-adjusted Spearman on raw daily values
        best_r, best_lag = -2.0, 0
        for lag in range(LAG_MIN, LAG_MAX + 1):
            # positive lag = gauge leads flood -> shift streamflow forward
            r = spearmanr(m["streamflow"].shift(lag), m["sfed"],
                          nan_policy="omit").statistic
            if r is not None and r > best_r:
                best_r, best_lag = r, lag
        thr = _threshold_at_rp(_annual_max(g, "streamflow"), cfg["rp_target"])
        rows.append({"gauge_id": gid, "best_r": round(best_r, 3),
                     "best_lag": best_lag, "rp_threshold": round(thr, 1)})
        daily_exceed[gid] = g.set_index("date")["streamflow"] > thr

    if not rows:
        return None, None
    res = pd.DataFrame(rows).sort_values("best_r", ascending=False).reset_index(drop=True)
    res.insert(0, "state", state)  # registry is keyed (gauge_id, state)
    res["is_selected"] = False
    res.loc[: TOP_N - 1, "is_selected"] = True
    res["lead_time"] = res["best_lag"].clip(lower=0)

    # evaluate the ≥consensus-of-N action trigger (reanalysis, year-round)
    sel = res[res["is_selected"]]["gauge_id"].tolist()
    n_req = max(1, int(np.ceil(cfg["consensus_frac"] * len(sel))))
    exc = pd.DataFrame({g: daily_exceed[g] for g in sel}).fillna(False)
    fires_per_day = exc.sum(axis=1)
    fire_dates = fires_per_day[fires_per_day >= n_req].index
    fire_years = set(_season_year(fire_dates))

    eval_years = set(range(y0, y1 + 1))
    ev = events & eval_years
    tp = len(fire_years & ev)
    fp = len(fire_years - ev)
    fn = len(ev - fire_years)
    tn = len(eval_years - fire_years - ev)
    perf = {
        "state": state, "n_gauges_selected": len(sel), "n_required": n_req,
        "rp_target": cfg["rp_target"], "consensus_frac": cfg["consensus_frac"],
        "event_years": ",".join(map(str, sorted(ev))),
        "fire_years": ",".join(map(str, sorted(fire_years))),
        "POD": round(tp / (tp + fn), 2) if (tp + fn) else None,
        "FPR": round(fp / (fp + tn), 2) if (fp + tn) else None,
        "n_events": len(ev), "n_fires": len(fire_years),
    }
    return res, perf


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", default="dev", choices=["dev", "prod"])
    p.add_argument("--states", nargs="*", default=None)
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args()

    gauge_reg = load_gauge_registry()
    sp = load_state_params().set_index("state")
    states = args.states or sorted(sp.index)

    all_res, perfs = [], []
    for state in states:
        print(f"\n=== {state} ===", flush=True)
        res, perf = process_state(state, sp.loc[state], gauge_reg)
        if res is None:
            print("  no GRRR gauges / insufficient data — skipped", flush=True)
            continue
        perfs.append(perf)
        all_res.append((state, res))
        top = res[res["is_selected"]]
        print(f"  top-{len(top)} gauges: best_r "
              f"{top['best_r'].min():.2f}–{top['best_r'].max():.2f}, "
              f"RP thresh {top['rp_threshold'].min():.0f}–{top['rp_threshold'].max():.0f} m³/s",
              flush=True)
        print(f"  events={perf['event_years']}  fires={perf['fire_years']}  "
              f"POD={perf['POD']} FPR={perf['FPR']}", flush=True)

    # merge results back into the gauge registry, keyed (gauge_id, state) — the
    # same gauge can serve several states with a different best_r/selection each
    fill = pd.concat([r for _, r in all_res], ignore_index=True)
    gr = gauge_reg.drop(
        columns=["best_r", "best_lag", "rp_threshold", "lead_time", "is_selected"],
        errors="ignore")
    gr = gr.merge(fill, on=["gauge_id", "state"], how="left")
    gr["is_selected"] = gr["is_selected"].fillna(False).astype(bool)
    perf_df = pd.DataFrame(perfs)

    print("\n=== per-state trigger performance ===", flush=True)
    print(perf_df.to_string(index=False), flush=True)

    if not args.no_upload:
        stratus.upload_parquet_to_blob(gr, GAUGE_REGISTRY_BLOB, stage=args.stage)
        stratus.upload_parquet_to_blob(perf_df, PERF_BLOB, stage=args.stage)
        print(f"\nUploaded enriched gauge registry + {PERF_BLOB}", flush=True)


if __name__ == "__main__":
    main()
