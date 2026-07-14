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

from pipelines.build_trigger_config import (  # noqa: E402
    GF_REFORECAST_BLOB_FMT,
    _season_year,
)
from src.config import load_gauge_registry  # noqa: E402
from src.constants import PROJECT_PREFIX  # noqa: E402
from src.datasources import grrr  # noqa: E402

SKILL_BLOB = PROJECT_PREFIX + "/processed/config/reforecast_skill.parquet"
# GRRR reforecast archive spans 2016-01-01..2023-06-30 -> full Apr-Mar seasons
SEASONS = range(2016, 2023)
# GloFAS reforecast archive: Jun-Dec issues, 2003-2023 -> seasons 2003-2022.
# No processed GloFAS reanalysis for most stations, so the reference for lead-L
# skill is the station's own lead-1 series (skill *decay*, not absolute skill —
# absolute usefulness is captured by best_r vs Floodscan in the registry).
GF_SEASONS = range(2003, 2023)


def _seasonal_peaks(df, value, seasons=SEASONS):
    """Max per full flood season, keyed by season start year."""
    out = df.copy()
    out["season"] = _season_year(out["valid_time"])
    out = out[out["season"].isin(seasons)]
    return out.groupby("season")[value].max()


def _skill_row(ra_pk, rf_pk, thr):
    """Rank/bias/threshold agreement between two seasonal-peak series."""
    m = pd.concat([ra_pk.rename("ra"), rf_pk.rename("rf")], axis=1).dropna()
    if len(m) < 3:
        return None
    rank_ra, rank_rf = m["ra"].rank(), m["rf"].rank()
    ra_over = set(m.index[m["ra"] > thr]) if pd.notna(thr) else set()
    rf_over = set(m.index[m["rf"] > thr]) if pd.notna(thr) else set()
    return dict(
        n_seasons=len(m),
        peak_rank_corr=round(rank_ra.corr(rank_rf), 3),
        rank_exact=bool((rank_ra == rank_rf).all()),
        peak_bias=round(float((m["rf"] / m["ra"]).median()), 3),
        thr_seasons_ra=",".join(map(str, sorted(ra_over))),
        thr_seasons_rf=",".join(map(str, sorted(rf_over))),
        thr_agree=bool(ra_over == rf_over),
    )


def check_glofas_station(state, station, thr):
    """GloFAS lead-L skill vs the station's own lead-1 series."""
    df = stratus.load_parquet_from_blob(
        GF_REFORECAST_BLOB_FMT.format(station=station))
    med = (df.groupby(["valid_time", "leadtime"])["dis24"].median()
           .reset_index())
    ref_pk = _seasonal_peaks(med[med["leadtime"] == 1], "dis24", GF_SEASONS)
    rows = []
    for lead, sub in med.groupby("leadtime"):
        r = _skill_row(ref_pk, _seasonal_peaks(sub, "dis24", GF_SEASONS), thr)
        if r:
            rows.append(dict(state=state, gauge_id=station, source="glofas",
                             reference="lead-1", leadtime=int(lead), **r))
    return rows


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
            r = _skill_row(ra_pk, _seasonal_peaks(sub, "streamflow"),
                           thr.get(gid))
            if r:
                rows.append(dict(state=state, gauge_id=gid, source="grrr",
                                 reference="reanalysis", leadtime=int(lead),
                                 **r))
    return pd.DataFrame(rows) if rows else None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage", default="dev", choices=["dev", "prod"])
    p.add_argument("--states", nargs="*", default=None)
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args()

    gauge_reg = load_gauge_registry()
    sel_all = gauge_reg[gauge_reg["is_selected"] == True]  # noqa: E712
    states = args.states or sorted(sel_all["state"].unique())
    all_gauges = sorted(
        sel_all[(sel_all["state"].isin(states))
                & (sel_all["source"] == "grrr")]["gauge_id"].unique())

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
        if res is not None:
            out.append(res)
            by_lead = res.groupby("leadtime")["peak_rank_corr"].median()
            n_exact = res[res["leadtime"] == 7]["rank_exact"].sum()
            print(f"{state:10s} median peak-rank ρ by lead: "
                  + " ".join(f"L{k}={v:.2f}" for k, v in by_lead.items())
                  + f"  (exact ranking @L7: {n_exact}/{res['gauge_id'].nunique()})",
                  flush=True)
        gf_sel = sel_all[(sel_all["state"] == state)
                         & (sel_all["source"] == "glofas")]
        for _, r in gf_sel.iterrows():
            rows = check_glofas_station(state, r["gauge_id"], r["rp_threshold"])
            if rows:
                gf = pd.DataFrame(rows)
                out.append(gf)
                mid = gf.set_index("leadtime")["peak_rank_corr"]
                print(f"{state:10s} glofas:{r['gauge_id']:12s} peak-rank ρ "
                      f"(vs lead-1): L4={mid.get(4, float('nan')):.2f} "
                      f"L8={mid.get(8, float('nan')):.2f} "
                      f"L16={mid.get(16, float('nan')):.2f}", flush=True)
        if res is None and gf_sel.empty:
            print(f"{state:10s} — no selected gauges, skipped", flush=True)

    skill = pd.concat(out, ignore_index=True)
    if not args.no_upload:
        stratus.upload_parquet_to_blob(skill, SKILL_BLOB, stage=args.stage)
        print(f"\nUploaded {SKILL_BLOB}", flush=True)


if __name__ == "__main__":
    main()
