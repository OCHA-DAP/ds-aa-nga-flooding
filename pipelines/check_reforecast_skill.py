"""Reforecast-vs-reanalysis skill check for the selected trigger gauges.

The historical trigger analysis runs on GRRR *reanalysis*; operationally the
trigger fires on Google *forecasts*. This is only defensible if the reforecast
reproduces the reanalysis at seasonal-peak level — ideally agreeing on the
historical *ranking* of seasonal peaks. For every selected gauge and lead time
(0–7 days), over the reforecast archive's full flood seasons (Apr–Mar,
2016–2022):

  * Spearman rank correlation of seasonal peaks (reforecast @ lead L vs
    reanalysis), and whether the ranking matches exactly
  * peak-magnitude bias (median reforecast peak / reanalysis peak)
  * threshold agreement: seasons where the reforecast crosses the gauge's
    trigger threshold vs seasons where the reanalysis does

Writes processed/config/reforecast_skill.parquet for the monitoring app.

    python pipelines/check_reforecast_skill.py --states Adamawa   # inspect one
    python pipelines/check_reforecast_skill.py                    # all states
"""

import argparse
import warnings

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

import ocha_stratus as stratus  # noqa: E402

from pipelines.build_trigger_config import _season_year  # noqa: E402
from src.config import load_gauge_registry  # noqa: E402
from src.constants import PROJECT_PREFIX  # noqa: E402
from src.datasources import grrr  # noqa: E402

SKILL_BLOB = PROJECT_PREFIX + "/processed/config/reforecast_skill.parquet"
# reforecast archive spans 2016-01-01..2023-06-30 -> full Apr-Mar seasons only
SEASONS = range(2016, 2023)


def _seasonal_peaks(df, value):
    """Max per full flood season, keyed by season start year."""
    out = df.copy()
    out["season"] = _season_year(out["valid_time"])
    out = out[out["season"].isin(SEASONS)]
    return out.groupby("season")[value].max()


def check_state(state, gauge_reg, ra, rf):
    sel = gauge_reg[(gauge_reg["state"] == state)
                    & (gauge_reg["source"] == "grrr")
                    & (gauge_reg["is_selected"] == True)]  # noqa: E712
    gauges = sel["gauge_id"].tolist()
    thr = dict(zip(sel["gauge_id"], sel["rp_threshold"]))
    if not gauges:
        return None

    rows = []
    for gid in gauges:
        ra_pk = _seasonal_peaks(ra[ra["gauge_id"] == gid], "streamflow")
        if ra_pk.empty:
            continue
        g_rf = rf[rf["gauge_id"] == gid]
        for lead, sub in g_rf.groupby("leadtime"):
            rf_pk = _seasonal_peaks(sub, "streamflow")
            m = pd.concat([ra_pk.rename("ra"), rf_pk.rename("rf")],
                          axis=1).dropna()
            if len(m) < 3:
                continue
            rank_ra = m["ra"].rank()
            rank_rf = m["rf"].rank()
            t = thr.get(gid)
            ra_over = set(m.index[m["ra"] > t]) if pd.notna(t) else set()
            rf_over = set(m.index[m["rf"] > t]) if pd.notna(t) else set()
            rows.append(dict(
                state=state, gauge_id=gid, leadtime=int(lead),
                n_seasons=len(m),
                peak_rank_corr=round(rank_ra.corr(rank_rf), 3),
                rank_exact=bool((rank_ra == rank_rf).all()),
                peak_bias=round(float((m["rf"] / m["ra"]).median()), 3),
                thr_seasons_ra=",".join(map(str, sorted(ra_over))),
                thr_seasons_rf=",".join(map(str, sorted(rf_over))),
                thr_agree=bool(ra_over == rf_over),
            ))
    return pd.DataFrame(rows) if rows else None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", default="dev", choices=["dev", "prod"])
    p.add_argument("--states", nargs="*", default=None)
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args()

    gauge_reg = load_gauge_registry()
    sel_all = gauge_reg[(gauge_reg["is_selected"] == True)  # noqa: E712
                        & (gauge_reg["source"] == "grrr")]
    states = args.states or sorted(sel_all["state"].unique())
    all_gauges = sorted(
        sel_all[sel_all["state"].isin(states)]["gauge_id"].unique())

    # load both archives once for the full gauge set (per-state loads would
    # re-download the same zarr chunks over and over)
    print(f"Loading reanalysis + reforecast for {len(all_gauges)} gauges…",
          flush=True)
    ra = grrr.process_reanalysis(grrr.load_reanalysis(gauge=all_gauges))
    ra["valid_time"] = pd.to_datetime(ra["valid_time"])
    rf = grrr.load_reforecast(gauge=all_gauges).to_dataframe().reset_index()
    rf["leadtime"] = rf["lead_time"].dt.days
    rf["valid_time"] = rf["issue_time"] + rf["lead_time"]

    out = []
    for state in states:
        res = check_state(state, gauge_reg, ra, rf)
        if res is None:
            print(f"{state:10s} — no selected gauges, skipped", flush=True)
            continue
        out.append(res)
        by_lead = res.groupby("leadtime")["peak_rank_corr"].median()
        n_exact = res[res["leadtime"] == 7]["rank_exact"].sum()
        print(f"{state:10s} median peak-rank ρ by lead: "
              + " ".join(f"L{k}={v:.2f}" for k, v in by_lead.items())
              + f"  (exact ranking @L7: {n_exact}/{res['gauge_id'].nunique()})",
              flush=True)

    skill = pd.concat(out, ignore_index=True)
    if not args.no_upload:
        stratus.upload_parquet_to_blob(skill, SKILL_BLOB, stage=args.stage)
        print(f"\nUploaded {SKILL_BLOB}", flush=True)


if __name__ == "__main__":
    main()
