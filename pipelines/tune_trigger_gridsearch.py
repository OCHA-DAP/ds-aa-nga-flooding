"""Phase 2 / Step 5 — per-state trigger grid search.

For each state, searches the trigger configuration space and picks the best:

  * per-gauge RP threshold level  ∈ {2, 3, 4, 5} year
  * consensus fraction            ∈ {0.3 … 1.0}
  * aggregation grouping          ∈ {state, basin}   (basin rescues confluence
                                     states whose gauges span >1 river arm)

All annual grouping is by flood season (Apr–Mar, labelled by start year — see
SEASON_START_MONTH in build_trigger_config) so Dec–Feb "black-flood" peaks on
the Niger aren't split across calendar years.

Each config is scored against the state's Floodscan 4-year event years by F1
(with POD/FPR/precision), tie-broken toward configs whose fire frequency matches
the event frequency (the "hold activation frequency" principle). The winning
config per state is written back to the gauge registry (per-gauge rp_threshold at
the chosen level, is_selected) and to state_params / trigger_performance.

    python pipelines/tune_trigger_gridsearch.py --states Kogi   # inspect one
    python pipelines/tune_trigger_gridsearch.py                 # all states
"""

import argparse
import warnings

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

import ocha_stratus as stratus  # noqa: E402

from pipelines.build_trigger_config import (  # noqa: E402
    GAUGE_SOURCES,
    _annual_max,
    _event_years,
    _fs_daily,
    _season_year,
    _threshold_at_rp,
    _weibull_rp,
    load_gauge_daily,
)
from src.config import load_gauge_registry, load_state_params  # noqa: E402
from src.config.registry import GAUGE_REGISTRY_BLOB, STATE_PARAMS_BLOB  # noqa: E402
from src.constants import PROJECT_PREFIX  # noqa: E402

# Search the per-gauge (individual) RP threshold freely; the OVERALL trigger
# return period is held constant by matching Adamawa's fire count (see main()).
RP_LEVELS = [2, 3, 4, 5]
FRACTIONS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
PERF_BLOB = PROJECT_PREFIX + "/processed/config/trigger_performance.parquet"
YEAR_DETAIL_BLOB = PROJECT_PREFIX + "/processed/config/trigger_year_detail.parquet"
BENCHMARK_RP = 4  # Floodscan event definition (design target)

# States whose config is pinned to the endorsed framework rather than the grid
# optimum (consistent with pinning Adamawa's LGAs to the endorsed 7).
ENDORSED = {"Adamawa": dict(rp_level=4, consensus_frac=0.6, grouping="state")}


def _f1(fire_years, events, all_years):
    tp = len(fire_years & events)
    fp = len(fire_years - events)
    fn = len(events - fire_years)
    tn = len(all_years - fire_years - events)
    pod = tp / (tp + fn) if (tp + fn) else 0.0
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    f1 = 2 * pod * prec / (pod + prec) if (pod + prec) else 0.0
    return dict(POD=round(pod, 2), FPR=round(fpr, 2), precision=round(prec, 2),
                F1=round(f1, 3), tp=tp, fp=fp, fn=fn)


def _fire_years(exceed_by_gauge, gauge_basin, frac, grouping):
    """Days meeting the consensus rule → set of fire years."""
    ex = pd.DataFrame(exceed_by_gauge).fillna(False)
    if grouping == "state":
        need = max(1, int(np.ceil(frac * ex.shape[1])))
        fire = ex.sum(axis=1) >= need
    else:  # basin: fire if ANY basin meets its own consensus
        fire = pd.Series(False, index=ex.index)
        for basin, gauges in _by_basin(ex.columns, gauge_basin).items():
            need = max(1, int(np.ceil(frac * len(gauges))))
            fire = fire | (ex[gauges].sum(axis=1) >= need)
    return set(_season_year(fire[fire].index))


def _by_basin(gauges, gauge_basin):
    out = {}
    for g in gauges:
        out.setdefault(gauge_basin[g], []).append(g)
    return out


def tune_state(state, cfg, gauge_reg, target_fires=None):
    fs = _fs_daily(state)
    events = _event_years(_annual_max(fs, "sfed"), BENCHMARK_RP)
    y0, y1 = int(cfg["analysis_start_year"]), int(cfg["analysis_end_year"])
    all_years = set(range(y0, y1 + 1))
    events &= all_years

    sub = gauge_reg[(gauge_reg["state"] == state)
                    & (gauge_reg["source"].isin(GAUGE_SOURCES))
                    & (gauge_reg["is_selected"] == True)]  # noqa: E712
    gauges = sub["gauge_id"].tolist()
    gauge_basin = dict(zip(sub["gauge_id"], sub["basin"]))
    if not gauges or not events:
        return None

    ra = load_gauge_daily(sub)
    ra_sy = _season_year(ra["date"])
    ra = ra[(ra_sy >= y0) & (ra_sy <= y1)]

    # per-gauge exceedance at each RP level, and the chosen-level threshold
    exceed = {rp: {} for rp in RP_LEVELS}
    thr_at = {rp: {} for rp in RP_LEVELS}
    n_basins = len(set(gauge_basin.values()))
    for g in gauges:
        s = ra[ra["gauge_id"] == g].set_index("date")["streamflow"].dropna()
        amax = _annual_max(s.reset_index(), "streamflow")
        for rp in RP_LEVELS:
            t = _threshold_at_rp(amax, rp)
            thr_at[rp][g] = round(t, 1)
            exceed[rp][g] = s > t

    def _cand(rp, frac, grouping, pinned=False):
        fy = _fire_years(exceed[rp], gauge_basin, frac, grouping)
        sc = _f1(fy, events, all_years)
        return dict(state=state, rp_level=rp, consensus_frac=frac,
                    grouping=grouping, pinned=pinned, n_basins=n_basins,
                    n_gauges=len(gauges), n_events=len(events), n_fires=len(fy),
                    event_years=",".join(map(str, sorted(events))),
                    fire_years=",".join(map(str, sorted(fy))), **sc)

    fs_am = _annual_max(fs, "sfed")
    fs_rp = _weibull_rp(fs_am)
    fs_daily = fs.set_index("date")["sfed"]

    def _daily_corr(rp):
        """Spearman ρ between the DAILY fraction of selected gauges over their
        threshold and the daily Floodscan SFED (rank-Pearson, no scipy)."""
        ex = pd.DataFrame(exceed[rp]).fillna(False)
        frac = ex.mean(axis=1).rename("frac")
        m = pd.concat([frac, fs_daily], axis=1).dropna()
        if len(m) < 30:
            return None
        return round(m["frac"].rank().corr(m["sfed"].rank()), 3)

    def _year_detail(cand):
        """One row per flood season: peak simultaneous gauge exceedance,
        Floodscan seasonal-peak SFED and its RP."""
        rp, frac = cand["rp_level"], cand["consensus_frac"]
        ex = pd.DataFrame(exceed[rp]).fillna(False)
        yrs = _season_year(ex.index)
        peak = ex.sum(axis=1).groupby(yrs).max()
        fired = set(int(y) for y in cand["fire_years"].split(",") if y)
        n_sel = len(gauges)
        n_req = max(1, int(np.ceil(frac * n_sel)))
        rows = []
        for y in range(y0, y1 + 1):
            r = fs_rp.get(y, float("nan"))
            has_r = r == r
            pk = fs_am.get(y, float("nan"))
            rows.append(dict(
                state=state, year=y, n_selected=n_sel,
                n_gauges_met=int(peak.get(y, 0)), n_required=n_req,
                fs_peak=round(pk, 4) if pk == pk else None,
                fs_rp=round(r, 2) if has_r else None,
                fired=(y in fired),
                is_event=(bool(r >= BENCHMARK_RP) if has_r else False)))
        return pd.DataFrame(rows)

    # anchor state uses the endorsed config; it defines the target frequency
    if state in ENDORSED and target_fires is None:
        e = ENDORSED[state]
        cand = _cand(e["rp_level"], e["consensus_frac"], e["grouping"], pinned=True)
        cand["daily_frac_corr"] = _daily_corr(cand["rp_level"])
        return cand, thr_at[e["rp_level"]], _year_detail(cand)

    # state-level consensus only, so "gauges met vs required" always explains
    # whether the trigger fired (a single interpretable N-of-M rule per state).
    groupings = ["state"]
    best = None
    for rp in RP_LEVELS:
        for frac in FRACTIONS:
            for grouping in groupings:
                cand = _cand(rp, frac, grouping)
                if target_fires is None:
                    key = (cand["F1"], -cand["FPR"],
                           -abs(cand["n_fires"] - len(events)))
                else:
                    # hold OVERALL frequency = Adamawa's; tie-break by detection F1
                    key = (-abs(cand["n_fires"] - target_fires),
                           cand["F1"], -cand["FPR"])
                if best is None or key > best[0]:
                    best = (key, cand, thr_at[rp])
    best[1]["daily_frac_corr"] = _daily_corr(best[1]["rp_level"])
    return best[1], best[2], _year_detail(best[1])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", default="dev", choices=["dev", "prod"])
    p.add_argument("--states", nargs="*", default=None)
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args()

    gauge_reg = load_gauge_registry()
    sp = load_state_params()
    states = args.states or sorted(sp["state"])
    sp_i = sp.set_index("state")

    # Adamawa's endorsed config sets the target activation frequency for all states
    ada, ada_thr, ada_yd = tune_state("Adamawa", sp_i.loc["Adamawa"], gauge_reg)
    target_fires = ada["n_fires"]
    print(f"target activation frequency = Adamawa's {target_fires} fire-years "
          f"(per-gauge RP searched freely per state)\n", flush=True)

    chosen, thresh_updates, year_details = [], {}, []
    for state in states:
        if state == "Adamawa":
            best, thr, yd = ada, ada_thr, ada_yd
        else:
            out = tune_state(state, sp_i.loc[state], gauge_reg,
                             target_fires=target_fires)
            if out is None:
                print(f"{state:10s} — no data / no events, skipped", flush=True)
                continue
            best, thr, yd = out
        chosen.append(best)
        # thresholds are per (gauge, state): the same gauge can serve two states
        # at different chosen RP levels
        thresh_updates.update({(g, state): t for g, t in thr.items()})
        year_details.append(yd)
        pin = " [PINNED endorsed]" if best.get("pinned") else ""
        print(f"{state:10s} rp={best['rp_level']} frac={best['consensus_frac']} "
              f"by={best['grouping']:5s}  F1={best['F1']:.2f} POD={best['POD']} "
              f"FPR={best['FPR']}  fires[{best['n_fires']}]={best['fire_years']}{pin}",
              flush=True)

    tuned = pd.DataFrame(chosen)
    # write chosen per-(gauge, state) thresholds (at each state's chosen RP level)
    gr = gauge_reg.copy()
    new_thr = pd.Series(
        [thresh_updates.get(k) for k in zip(gr["gauge_id"], gr["state"])],
        index=gr.index, dtype="float64")
    gr["rp_threshold"] = new_thr.fillna(gr["rp_threshold"])
    # per-state chosen levers into state_params
    tuned_cfg = tuned[["state", "rp_level", "consensus_frac", "grouping"]].rename(
        columns={"rp_level": "rp_target", "grouping": "group_by"})
    stale = [c for c in sp.columns
             if c in ("rp_target", "consensus_frac", "group_by")
             or c.endswith(("_x", "_y"))]
    sp2 = sp.drop(columns=stale, errors="ignore").merge(
        tuned_cfg, on="state", how="left")

    print("\n=== tuned trigger configs ===", flush=True)
    print(tuned[["state", "rp_level", "consensus_frac", "grouping", "pinned",
                 "POD", "FPR", "F1", "n_events", "n_fires"]].to_string(index=False),
          flush=True)

    year_detail = pd.concat(year_details, ignore_index=True)

    if not args.no_upload:
        stratus.upload_parquet_to_blob(gr, GAUGE_REGISTRY_BLOB, stage=args.stage)
        stratus.upload_parquet_to_blob(sp2, STATE_PARAMS_BLOB, stage=args.stage)
        stratus.upload_parquet_to_blob(tuned, PERF_BLOB, stage=args.stage)
        stratus.upload_parquet_to_blob(year_detail, YEAR_DETAIL_BLOB, stage=args.stage)
        print("\nUploaded gauge registry, state_params, trigger_performance, "
              "trigger_year_detail", flush=True)


if __name__ == "__main__":
    main()
