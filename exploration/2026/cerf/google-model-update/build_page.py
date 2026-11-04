"""Build exploration/2026/cerf/google-model-update/index.html.

The endorsed 2026 Adamawa action trigger replayed on Google's November 2026
model with the method held fixed, next to the model it was designed on.
Numbers come from analysis.py (same folder); downloads are cached in
temp/google-model-update/.

    PYTHONPATH=. .venv/bin/python exploration/2026/cerf/google-model-update/build_page.py
"""

import html
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import analysis as A  # noqa: E402
from sm_chart import SM_CSS, SM_JS, sm_panel  # noqa: E402

C_OLD, C_NEW, C_FS = "#2a78d6", "#eb6834", "#1baf7a"  # validated (dataviz validate_palette.js, light)
# Every old-vs-new comparison uses only the years both versions cover: the record 1998-2023 (the
# calibration years) and, for forecasts, the seasons both archives cover completely (2016-2022).
# The new model's 2023-2025 forecasts and 2024-2025 record are reported separately.
YEARS = list(range(A.START, A.END + 1))
N_YEARS = A.END - A.START + 1
API_CHECKED = "4 November 2026"
WUROBOKI = "hybas_1120842550"
OUTLIER = "hybas_1120840690"


def e(x):
    return html.escape(str(x))


def yrs(v):
    v = sorted(v)
    return ", ".join(str(y) for y in v) if v else "none"


def rp_text(k, n=N_YEARS):
    return f"1-in-{(n + 1) / k:.1f}" if k else "never"


def d(x):
    return f"{x.day} {x.strftime('%b')}" if x is not None and not pd.isna(x) else "&ndash;"


def plural(k):
    return f"{k} day" if k == 1 else f"{k} days"


def lag(k):
    return f"{k:+d} d" if k else "0 d"


def change(a, b):
    """New minus old, in days; positive = the new model is later."""
    if a is None or b is None:
        return "&ndash;"
    k = (pd.Timestamp(b) - pd.Timestamp(a)).days
    return "same day" if k == 0 else (f"{plural(k)} later" if k > 0 else f"{plural(-k)} earlier")


def ahead(trig, ref):
    """Days the trigger day/issue came before the FloodScan reference day."""
    if trig is None or ref is None:
        return None
    return (pd.Timestamp(ref) - pd.Timestamp(trig)).days


def ahead_txt(k):
    if k is None:
        return "&ndash;"
    return "same day" if k == 0 else (f"{plural(k)} before" if k > 0 else f"{plural(-k)} after")


def grid(rows, title):
    head = "".join(f"<th>{str(y)[2:]}</th>" for y in YEARS)
    body = ""
    for label, cells in rows:
        tds = ""
        for y in YEARS:
            txt, cls, tip = cells.get(y, ("", "nd", f"{y}: no data"))
            tds += f'<td class="{cls}" title="{e(tip)}">{txt}</td>'
        body += f"<tr><th class=rl>{label}</th>{tds}</tr>"
    return (f'<div class="tablewrap"><table class="yg" aria-label="{e(title)}"><thead><tr><th class=rl></th>{head}'
            f"</tr></thead><tbody>{body}</tbody></table></div>")


def main():
    res = A.run()
    old, new = res["old"], res["new"]
    fs_ev, fs_first, fs_ratio = res["fs_events"], res["fs_first"], res["fs_ratio"]
    fs_years = set(fs_ratio.index.year)

    # ------------------------------------------------------------ year grid
    rows = []
    cells = {}
    for y in YEARS:
        if y not in fs_years:
            continue
        top = max([rp for rp in A.FS_RP_LEVELS if y in fs_ev[rp]], default=None)
        cells[y] = ((str(top), "sev" if top and top >= 4 else "fl", f"{y}: FloodScan 1-in-{top} flood year or rarer")
                    if top else ("", "no", f"{y}: below the FloodScan 1-in-3 level"))
    rows.append(("FloodScan flood year", cells))
    rf_eval = old["rf_complete"]  # forecast seasons both archives cover completely
    assert set(rf_eval) <= set(new["rf_complete"])
    for r, lab in ((old, "Old model"), (new, "New model")):
        n_exc = r["n_exc"]
        cells = {}
        for y, k in n_exc.items():
            k = int(k)
            act = k >= A.N_GAUGES
            cells[int(y)] = (str(k), "act" if act else ("z" if k == 0 else "part"),
                             f"{y}: up to {k} of 10 gauges over their 1-in-4 level on one day" + (" (rule met)" if act else ""))
        rows.append((f"{lab}: gauges over", cells))
    for r, lab in ((old, "Old model"), (new, "New model")):
        ff = r["rf_first_forecast"]
        yrs_hit = set(r["rf_years"]) & set(rf_eval)
        cells = {}
        for y in rf_eval:
            cells[y] = (("&#9679;", "act", f"{y}: a forecast issued {d(ff[y][0])} met the rule") if y in yrs_hit
                        else ("", "z", f"{y}: no forecast at leads 0-7 met the rule"))
        rows.append((f"{lab}: forecast", cells))
    year_grid = grid(rows, "Adamawa action trigger by year")

    # ------------------------------------------------------------ timing
    ffo = {y: v for y, v in old["rf_first_forecast"].items() if y in rf_eval}
    ffn = {y: v for y, v in new["rf_first_forecast"].items() if y in rf_eval}
    assert all(y in YEARS for y in list(old["first_crossing"]) + list(new["first_crossing"]))
    t_years = sorted(set(old["first_crossing"]) | set(new["first_crossing"]) | set(ffo) | set(ffn))
    t_rows = ""
    leads = {}
    for y in t_years:
        oc, nc = old["first_crossing"].get(y), new["first_crossing"].get(y)
        oi, ni = ffo.get(y, (None,))[0], ffn.get(y, (None,))[0]
        ref = fs_first.get(y)
        top = max([rp for rp in A.FS_RP_LEVELS if y in fs_ev[rp]], default=None)
        cls = (f"1-in-{top}" if top else "below 1-in-3") if y in fs_years else "not covered"
        leads[y] = {"old_issue": ahead(oi, ref), "new_issue": ahead(ni, ref)}
        t_rows += (f"<tr><td><b>{y}</b></td><td class=n>{d(oc)}</td><td class=n>{d(nc)}</td><td>{change(oc, nc)}</td>"
                   f"<td class=n>{d(oi)}</td><td class=n>{d(ni)}</td><td>{change(oi, ni)}</td>"
                   f"<td class=n>{d(ref)}</td><td>{cls}</td></tr>")

    # ------------------------------------------------------------ chart
    panels = []
    for y in t_years:
        oc, nc = old["first_crossing"].get(y), new["first_crossing"].get(y)
        note = f"rule met: old {d(oc)} &middot; new {d(nc)}".replace("&ndash;", "no")
        panels.append(sm_panel(str(y), note, f"{y}-08-01", f"{y}-11-30",
                               {"old": old["margin"], "new": new["margin"], "fs": fs_ratio},
                               marks={"old": oc, "new": nc}))
    spec = {"yMax": 2.0, "yTicks": [0, 0.5, 1, 1.5, 2], "aria": "share of the 1-in-4 level, old and new model, FloodScan",
            "series": [{"key": "old", "label": "Old model", "color": C_OLD},
                       {"key": "new", "label": "New model", "color": C_NEW},
                       {"key": "fs", "label": "FloodScan", "color": C_FS, "dash": "4 3"}],
            "panels": panels}

    # ------------------------------------------------------------ levels
    lv = pd.DataFrame({"old": old["thr"], "new": new["thr"]})
    sel = res["gauges"].set_index("gauge_id")
    l_rows = ""
    for g, x in lv.iterrows():
        ro, lo = old["corr"][g]
        rn, ln = new["corr"][g]
        tag = " (Wuroboki, quality-verified)" if g == WUROBOKI else (" (small tributary)" if g == OUTLIER else "")
        l_rows += (f"<tr><td>{g.removeprefix('hybas_')}{tag}</td><td class=n>{sel.loc[g, 'latitude']:.3f}, {sel.loc[g, 'longitude']:.3f}</td>"
                   f"<td class=n>{x.old:,.1f}</td><td class=n><em>{x.new:,.1f}</em></td><td class=n>{x.new / x.old:.2f}x</td>"
                   f"<td class=n>{ro:.2f} ({lag(lo)})</td><td class=n>{rn:.2f} ({lag(ln)})</td></tr>")
    main_ratio = (lv.new / lv.old).drop(OUTLIER)

    # ------------------------------------------------------------ scores
    def sc(s):
        f = lambda v: "&ndash;" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.2f}"
        return f"<td class=n>{s['TP']}</td><td class=n>{s['FP']}</td><td class=n>{s['FN']}</td><td class=n>{f(s['POD'])}</td><td class=n>{f(s['FAR'])}</td>"
    new_rf_shared = [y for y in new["rf_years"] if y in rf_eval]
    mismatch = [y for y in new["mismatch_years"] if y in YEARS]  # old levels on new flows, shared years
    s_rows = ""
    for rp in A.FS_RP_LEVELS:
        s_rows += (f"<tr><td>1-in-{rp}</td><td>record, {A.START}&ndash;{A.END}</td>{sc(old['scores'][rp])}{sc(new['scores'][rp])}</tr>")
    for rp in A.FS_RP_LEVELS:
        s_rows += (f"<tr><td>1-in-{rp}</td><td>forecast, {rf_eval[0]}&ndash;{rf_eval[-1]}</td>{sc(old['rf_scores'][rp])}"
                   f"{sc(A.score(new_rf_shared, fs_ev[rp], rf_eval))}</tr>")
    sweep = "".join(f"<tr{' class=pick' if rp == A.RP_THRESH else ''}><td>1-in-{rp}</td><td>{yrs(old['sweep'][rp])}</td>"
                    f"<td>{yrs(new['sweep'][rp])}</td></tr>" for rp in (3, 4, 5, 6))

    # ------------------------------------------------------------ headline checks (the text states these)
    assert old["years"] == new["years"]
    shifts = {y: (new["first_crossing"][y] - old["first_crossing"][y]).days for y in old["years"]}
    fshifts = {y: (ffn[y][0] - ffo[y][0]).days for y in set(ffo) & set(ffn)}
    assert set(new_rf_shared) - set(old["rf_years"]) == {2016} and 2016 not in old["years"] + new["years"]
    assert not new["ext_years"] and int(new["ext_n_exc"].max()) <= 1
    assert 2016 not in fs_ev[3] and 2016 not in fs_ev[4]
    assert {g: v[1] for g, v in old["corr"].items() if g != OUTLIER} == {g: -3 for g in old["corr"] if g != OUTLIER}
    assert all(v[1] == 0 for g, v in new["corr"].items() if g != OUTLIER)
    assert all(lv.new[g] > 3000 and lv.old[g] < 1200 for g in lv.index if g != OUTLIER)
    peak25 = float(new["margin"][new["margin"].index.year == 2025].max())
    no_rows = ""
    for y in (2023, 2024, 2025):
        ra_k = "shared record, see above" if y <= A.END else f"{int(new['ext_n_exc'][y])} of 10"
        pk = "" if y <= A.END else f"{float(new['margin'][new['margin'].index.year == y].max()):.0%} of its level"
        met = (y in new["ext_years"]) or (y in new["ext_rf_years"])
        no_rows += (f"<tr><td><b>{y}</b></td><td>{ra_k}</td><td>{pk}</td><td>{int(new['ext_rf_n_exc'][y])} of 10</td>"
                    f"<td>{'yes' if met else 'no'}</td></tr>")
    assert not any(y in new["ext_rf_years"] for y in (2023, 2024, 2025))
    assert len(mismatch) == len(YEARS), "the callout says every season"
    assert {2024, 2025} <= set(new["mismatch_years"]), "the callout says 2024 and 2025 too"
    oc_, nc_ = [v[0] for v in old["corr"].values()], [v[0] for v in new["corr"].values()]
    assert min(nc_) > min(oc_) and max(nc_) > max(oc_), "the text says slightly more closely"
    # the single forecast issued on the first 2016 date put 9 of 10 gauges over on one valid day
    rf16 = A._rf_frame(new["rf"])
    rf16 = rf16[(rf16.issue_time == ffn[2016][0]) & rf16.leadtime.le(A.MAX_LEAD_TIME) & rf16.gauge_id.isin(new["thr"].index)]
    rf16 = rf16.join(new["thr"].rename("t"), on="gauge_id")
    n16 = int((rf16.streamflow > rf16.t).groupby(rf16.valid_time).sum().max())
    assert n16 == 9, n16
    early = min(shifts.values())
    late = max(shifts.values())
    y_early = min(shifts, key=shifts.get)
    y_late = max(shifts, key=shifts.get)
    assert new["first_crossing"][y_late] > fs_first[y_late]

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>New Google model and the Adamawa action trigger &mdash; Nigeria flooding</title>
<meta name="description" content="Google released a new version of its flood model in November 2026. The endorsed 2026 Adamawa action trigger replayed on it with the same method: activation years, timing and gauge levels, old against new.">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Merriweather:wght@700&family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root {{ --b5:#269777; --b6:#1e795f; --b7:#18614c; --b05:#e9f5f1; --b1:#d4eae4; --n9:#1f2324; --n8:#3f4748; --n7:#5e6a6b; --n05:#f5f7f7; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--n05); color:var(--n9); font-family:'Roboto',system-ui,-apple-system,'Segoe UI','Helvetica Neue',Arial,sans-serif; line-height:1.5; }}
.wrap {{ max-width:1080px; margin:0 auto; background:#fff; min-height:100vh; box-shadow:0 0 40px rgba(31,35,36,.06); }}
.hero {{ background:var(--b6); padding:40px 44px 34px; }}
.hero h1 {{ font-family:'Merriweather',Georgia,serif; font-weight:700; font-size:28px; line-height:1.2; color:#fff; margin:0 0 14px; }}
.hero p {{ margin:0; font-size:15px; line-height:1.6; color:#fff; max-width:66ch; }}
.crumb {{ font-size:12px !important; margin:0 0 14px !important; }} .crumb a {{ color:rgba(255,255,255,.85); text-decoration:none; }}
article {{ padding:8px 44px 24px; max-width:960px; }}
article h2 {{ font-family:'Merriweather',Georgia,serif; font-size:21px; color:var(--n9); margin:40px 0 10px; line-height:1.25; }}
article h2 .num {{ color:var(--b5); font-size:15px; margin-right:8px; }}
article h3 {{ font-size:15px; color:var(--n9); margin:26px 0 6px; }}
article p, article li {{ font-size:14.5px; color:var(--n8); line-height:1.65; }} article a {{ color:var(--b6); }} article strong {{ color:var(--n9); }}
article code {{ background:var(--n05); border:1px solid #e2e7e7; padding:1px 5px; border-radius:3px; font-size:12.5px; }}
table.data {{ border-collapse:collapse; width:100%; margin:14px 0; font-size:13px; }}
table.data th {{ text-align:left; font-size:11px; text-transform:uppercase; letter-spacing:.07em; color:var(--n7); font-weight:700; border-bottom:2px solid var(--b1); padding:7px 10px 6px; vertical-align:bottom; }}
table.data td {{ border-bottom:1px solid #eef1f1; padding:7px 10px; color:var(--n8); vertical-align:top; }}
table.data td.n {{ text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }} table.data td em {{ font-style:normal; color:var(--b6); font-weight:600; }}
table.data th.grp {{ text-align:center; border-bottom:1px solid var(--b1); }}
table.data tr.pick td {{ background:var(--b05); }} .tablewrap {{ overflow-x:auto; }}
table.yg {{ border-collapse:separate; border-spacing:2px; margin:12px 0 6px; font-size:11.5px; }}
table.yg th {{ font-weight:500; color:#6b7683; font-size:10.5px; padding:0 0 2px; text-align:center; }}
table.yg th.rl {{ text-align:right; padding-right:8px; white-space:nowrap; color:var(--n8); font-size:12px; font-weight:500; }}
table.yg td {{ width:24px; min-width:24px; height:22px; text-align:center; border-radius:3px; background:#f3f5f7; color:#9aa4ae; font-variant-numeric:tabular-nums; }}
table.yg td.nd {{ background:repeating-linear-gradient(45deg,#f6f7f8 0 3px,#e9ecef 3px 5px); }}
table.yg td.z {{ color:#c4cbd2; }} table.yg td.part {{ color:var(--n8); background:#e6ebf0; }}
table.yg td.act {{ background:#1f5c8b; color:#fff; font-weight:700; }}
table.yg td.fl {{ background:#f6dfb8; color:#7a4d06; font-weight:700; }} table.yg td.sev {{ background:#d9822b; color:#fff; font-weight:700; }}
.stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:14px; margin:26px 0 6px; }}
.stat {{ background:#fff; border:1px solid #e2e7e7; border-top:3px solid var(--b5); border-radius:5px; padding:14px 16px 12px; }}
.stat.flag {{ border-top-color:#e0a04b; }} .stat .v {{ font-family:'Merriweather',Georgia,serif; font-size:23px; font-weight:700; color:var(--b6); line-height:1.15; }}
.stat.flag .v {{ color:#b0722b; }} .stat .l {{ font-size:11px; text-transform:uppercase; letter-spacing:.08em; color:var(--n7); margin-top:6px; line-height:1.45; }}
.callout {{ margin:20px 0; padding:13px 17px; border-radius:4px; background:var(--b05); border-left:5px solid var(--b5); font-size:13.5px; color:var(--n8); line-height:1.6; }}
.callout.warn {{ background:#fdf3e7; border-left-color:#e0a04b; }}
.figcap {{ font-size:12px !important; color:var(--n7) !important; line-height:1.5 !important; }}
details {{ margin:10px 0 0; font-size:13.5px; }} details summary {{ cursor:pointer; color:var(--b6); }}
.provenance {{ margin:34px 44px 40px; padding-top:14px; border-top:1px solid #e2e7e7; font-size:12px; color:var(--n7); line-height:1.7; }} .provenance a {{ color:var(--b6); }}
@media (max-width:640px) {{ article {{ padding:4px 22px 16px; }} .hero {{ padding:32px 22px 28px; }} .provenance {{ margin:28px 22px 32px; }} }}
{SM_CSS}
</style>
<script>{SM_JS}</script>
</head>
<body>
<div class="wrap">
  <header class="hero">
    <p class="crumb"><a href="../../../../">Nigeria flooding &mdash; anticipatory action</a> / operational note</p>
    <h1>The new Google model and the Adamawa action trigger</h1>
    <p>Google has released a new version of the flood model behind Flood Hub (November 2026). The 2026 action trigger runs on
       Google: at least 6 of 10 gauges on the Benue in Adamawa over their own 1-in-4 level on the same day. This page replays
       it on the new model with exactly the method used to design it on the old one: the same ten gauges, the same
       August&ndash;November season, the same 1998&ndash;2023 calibration years, the same level and gauge count. Only the
       per-gauge levels are refitted, on the new model's own record. The readiness trigger runs on GloFAS and does not
       change. {API_CHECKED}.</p>
  </header>

  <article>
    <div class="stats">
      <div class="stat"><div class="v">{len(old['years'])} &rarr; {len(new['years'])}</div><div class="l">activations {A.START}&ndash;{A.END}, old &rarr; new: the same six years</div></div>
      <div class="stat"><div class="v">{main_ratio.min():.1f}&ndash;{main_ratio.max():.1f}x</div><div class="l">higher 1-in-4 levels at the nine main-channel gauges</div></div>
      <div class="stat"><div class="v">{abs(early)} earlier &ndash; {late} later</div><div class="l">days: change in the first day the rule is met ({y_early}, {y_late})</div></div>
      <div class="stat flag"><div class="v">{len(mismatch)} of {len(YEARS)}</div><div class="l">seasons {A.START}&ndash;{A.END} activated if the old levels met new-model flows</div></div>
    </div>

    <h2><span class="num">1</span>What changes</h2>
    <ul>
      <li><strong>Activation years: none.</strong> Both models meet the rule in {yrs(old['years'])}: {rp_text(len(old['years']))}, as in the
          framework. Scores against FloodScan are therefore identical (section 5).</li>
      <li><strong>Timing within the season does change.</strong> The first day the rule is met moves by
          {", ".join(f"{change(old['first_crossing'][y], new['first_crossing'][y])} in {y}" for y in old['years'] if shifts[y] != 0)}
          (same day in {yrs([y for y in old['years'] if shifts[y] == 0])}). The two large moves are {y_late}, which the new model reaches {late} days
          after the old model and after FloodScan's first day over its own 1-in-4 level, and {y_early}, which it reaches {abs(early)} days earlier.</li>
      <li><strong>Forecasts</strong> (archive {rf_eval[0]}&ndash;{rf_eval[-1]} on both models, leads 0&ndash;7 days): the old model's forecasts meet the
          rule in {yrs(old['rf_years'])}, the new model's in {yrs(new_rf_shared)}. 2016 is new: a forecast issued {d(ffn[2016][0])} put {n16} of 10 gauges over
          their level, while neither model's record nor FloodScan show a 1-in-4 flood that year. The first qualifying forecast moves by
          {", ".join(f"{change(ffo[y][0], ffn[y][0])} in {y}" for y in sorted(fshifts))}.</li>
      <li><strong>Levels.</strong> The new model runs about three times higher at these gauges: 1-in-4 levels of {lv.new.drop(OUTLIER).min():,.0f}&ndash;{lv.new.drop(OUTLIER).max():,.0f} m³/s
          against {lv.old.drop(OUTLIER).min():,.0f}&ndash;{lv.old.drop(OUTLIER).max():,.0f} on the nine main-channel gauges. Its August&ndash;November flows follow FloodScan
          slightly more closely (best correlation {min(v[0] for v in new['corr'].values()):.2f}&ndash;{max(v[0] for v in new['corr'].values()):.2f} against {min(v[0] for v in old['corr'].values()):.2f}&ndash;{max(v[0] for v in old['corr'].values()):.2f}),
          and match it best with no time offset on the main channel, where the old model's best match was offset by three days.</li>
      <li><strong>Same years throughout.</strong> Every old-versus-new comparison on this page uses only the years both versions cover:
          the record {A.START}&ndash;{A.END} (the calibration years) and, for forecasts, the seasons {rf_eval[0]}&ndash;{rf_eval[-1]} that both
          archives cover completely. What the new model says about 2023&ndash;2025 beyond that is in section 6: the rule is not met in
          2024 or 2025.</li>
    </ul>

    <div class="callout warn"><strong>Before Google switches Flood Hub to the new model.</strong> On {API_CHECKED} the Flood Hub API still serves
      the old model: Wuroboki's warning, danger and extreme-danger levels are unchanged (1,094, 1,352 and 1,648 m³/s) and gauges keep their
      <code>hybas_</code> identifiers. The monitoring reads Google against the endorsed levels (about 1,100&ndash;1,120 m³/s on the main channel). The new model's
      ordinary wet-season flows sit far above those levels: applied to the new record they would have met the rule in
      <strong>every one of the {len(YEARS)} seasons {A.START}&ndash;{A.END}</strong> (and in 2024 and 2025 too). The levels have to be replaced by the refitted ones (section 4) when the
      API switches, and before that if Google announces a date. Signals that the switch has happened: the warning levels or gauge model
      identifiers change for these gauges, or identifiers move from <code>hybas_</code> to <code>subhybas_</code> as in the new dataset (not confirmed).</div>

    <h2><span class="num">2</span>The two versions</h2>
    <div class="tablewrap"><table class="data"><thead><tr><th></th><th>Old model (design)</th><th>New model (November 2026)</th></tr></thead><tbody>
      <tr><td>Record (reanalysis)</td><td>1980 &ndash; Dec 2023</td><td>Dec 1980 &ndash; Dec 2025</td></tr>
      <tr><td>Forecast archive (reforecast)</td><td>issued Jan 2016 &ndash; Jun 2023, leads 0&ndash;7 days</td><td>issued Jan 2016 &ndash; Dec 2025, leads 0&ndash;10 days</td></tr>
      <tr><td>Model</td><td>lumped, one model per basin</td><td>semi-distributed, routed along the HydroBASINS river network</td></tr>
      <tr><td>Gauges</td><td colspan=2>the same ten HydroBASINS basins, with identical outlet coordinates</td></tr>
    </tbody></table></div>
    <p>Both versions label days the same way (the record for day T equals the forecast issued on day T+1 at lead 0), so the design's reading
       of forecast dates applies to both unchanged. Leads 8&ndash;10 of the new forecast archive are not used here.</p>

    <h2><span class="num">3</span>When the trigger is met</h2>
    <p>One column per year. FloodScan row: the rarest FloodScan flood level reached in Adamawa that year (3, 4 or 5 for 1-in-3, 1-in-4,
       1-in-5; the benchmark the design was scored on). Model rows: the most gauges over their own 1-in-4 level on any
       one day of August&ndash;November; filled where the rule is met (6 or more). Forecast rows: a year in which the forecasts at leads of
       0 to 7 days put 6 or more gauges over their level on the same day (the design's reading: the highest forecast value per
       gauge and day across issues). Years {A.START}&ndash;{A.END}, the years both versions cover; forecasts {rf_eval[0]}&ndash;{rf_eval[-1]},
       the seasons both forecast archives cover completely (hatched otherwise). Hover a cell for details.</p>
    {year_grid}
    <h3>Dates</h3>
    <p>First day each model's record meets the rule, and first forecast issue date that meets it. The last two columns are the reference the
       design used for timing: the first day FloodScan crossed its own 1-in-4 level that season, and the FloodScan class of the year.</p>
    <div class="tablewrap"><table class="data"><thead><tr><th>Year</th><th>Record: old</th><th>Record: new</th><th>Change</th>
      <th>Forecast: old</th><th>Forecast: new</th><th>Change</th><th>FloodScan over 1-in-4</th><th>FloodScan year</th></tr></thead>
      <tbody>{t_rows}</tbody></table></div>
    <p>Against FloodScan's first day over its 1-in-4 level, the first qualifying forecast came
       {"; ".join(f"{y}: old {ahead_txt(leads[y]['old_issue'])}, new {ahead_txt(leads[y]['new_issue'])}" for y in t_years if leads[y]['old_issue'] is not None or leads[y]['new_issue'] is not None)}.</p>
    <div class="sm-legend"><span><span class="sw" style="background:{C_OLD}"></span>Old model</span>
      <span><span class="sw" style="background:{C_NEW}"></span>New model</span>
      <span><span class="sw dash" style="border-color:{C_FS}"></span>FloodScan</span>
      <span><span class="sw dash" style="border-color:#5c6b7a"></span>the 1-in-4 level (100%)</span></div>
    <div class="sm-grid" id="sm-nga"></div>
    <p class="figcap">Daily flow at the sixth-highest of the ten gauges, as a share of that gauge's own 1-in-4 level, August to November, on each
       model's record; the rule is met on days the line is above 100%, and dots mark the first such day. FloodScan: flooded fraction in Adamawa
       as a share of its own 1-in-4 level. Years shown: every year from 1998 to 2023 in which either model or either forecast met the rule.</p>
    <script>drawSM(document.getElementById('sm-nga'), {json.dumps(spec)});</script>

    <h2><span class="num">4</span>Gauge levels</h2>
    <p>1-in-4 level at each gauge fitted on the August&ndash;November annual maxima 1998&ndash;2023 of each model's own record (Weibull plotting
       position with log-log interpolation, as in the design), and each gauge's best rank correlation with FloodScan over lags of &minus;3 to +14
       days (the gauge-selection measure). The old levels are the endorsed ones; the new levels are what the monitoring would need once Flood
       Hub serves the new model (m³/s).</p>
    <div class="tablewrap"><table class="data"><thead><tr><th>Gauge (HydroBASINS)</th><th>Outlet</th><th>Old level</th><th>New level</th><th>New / old</th>
      <th>Old correlation (lag)</th><th>New correlation (lag)</th></tr></thead><tbody>{l_rows}</tbody></table></div>

    <h2><span class="num">5</span>Performance against FloodScan</h2>
    <p>As scored in the design: a year counts as a hit when the trigger is met in a FloodScan flood year at the given level, a false alarm when it
       is met in any other year. Record: {A.START}&ndash;{A.END}. Forecast: the years both forecast archives cover with the season complete.</p>
    <div class="tablewrap"><table class="data"><thead>
      <tr><th rowspan=2>FloodScan level</th><th rowspan=2>Series</th><th class=grp colspan=5>Old model</th><th class=grp colspan=5>New model</th></tr>
      <tr><th>Hits</th><th>False</th><th>Missed</th><th>POD</th><th>FAR</th><th>Hits</th><th>False</th><th>Missed</th><th>POD</th><th>FAR</th></tr></thead>
      <tbody>{s_rows}</tbody></table></div>
    <details><summary>Other return periods, same 6-of-10 rule</summary>
    <div class="tablewrap"><table class="data"><thead><tr><th>Level</th><th>Old model, years</th><th>New model, years</th></tr></thead>
      <tbody>{sweep}</tbody></table></div></details>

    <h2><span class="num">6</span>New model only: 2023&ndash;2025</h2>
    <p>The new record runs two years, and the new forecast archive two and a half seasons, past the old ones. These years have no
       old-model counterpart and are kept out of every comparison above. Levels are still those fitted on {A.START}&ndash;{A.END}.
       FloodScan in this benchmark ends in 2023.</p>
    <div class="tablewrap"><table class="data"><thead><tr><th>Year</th><th>Record: most gauges over on one day</th>
      <th>Record: sixth-highest gauge at its peak</th><th>Forecasts: most gauges over (leads 0&ndash;7)</th><th>Rule met</th></tr></thead>
      <tbody>{no_rows}</tbody></table></div>
    <p>The framework did activate on 7&ndash;8 September 2025, under the 2025 version's single-gauge rule on the forecasts of the time.
       The 2026 rule on the new model is not met in 2025: at most one of the ten gauges went over its level on any day, and the
       sixth-highest peaked at {peak25:.0%} of its level. The old model's archive ends in 2023, so it cannot be compared for 2025.</p>

    <h2><span class="num">7</span>Method</h2>
    <ul>
      <li>The design notebook's steps (<code>exploration/2026/cerf/workflow/06_trigger_definition.ipynb</code>) run on each model's data in turn, in
          <code>analysis.py</code> next to this page: the estimator is copied verbatim; a gauge counts when strictly above its level; forecast valid
          day = issue day + lead, leads 0&ndash;7, best value per day across issues, years whose forecasts reach October.</li>
      <li>The old model run here reproduces the endorsed levels to 0.1 m³/s and the endorsed years (record 1998, 1999, 2012, 2018, 2019, 2022;
          forecast 2018, 2019, 2022).</li>
      <li>Same years for both versions: levels fitted on {A.START}&ndash;{A.END} for both; activations, timing, FloodScan scores, correlations
          and the old-levels-on-new-flows count all over {A.START}&ndash;{A.END}; forecasts over the complete seasons {rf_eval[0]}&ndash;{rf_eval[-1]}.</li>
      <li>The gauge set is the endorsed one; gauge selection was not re-run. FloodScan benchmark years are the design's.</li>
      <li>Data: Google Runoff Reanalysis &amp; Reforecast, public Google Cloud bucket <code>flood-forecasting</code>, old version
          <code>model_id_8583a5c2_v0</code>, new version <code>distributed_model_2026_11</code> (CC-BY-4.0).</li>
    </ul>
  </article>

  <p class="provenance">Generated {pd.Timestamp.today().day} {pd.Timestamp.today().strftime('%B %Y')} by
    <code>exploration/2026/cerf/google-model-update/build_page.py</code> in
    <a href="https://github.com/OCHA-DAP/ds-aa-nga-flooding">ds-aa-nga-flooding</a>.
    Related: <a href="../notes/">2026 CERF analysis notes</a>, <a href="../monitoring/">2026 monitoring status</a>.</p>
</div>
</body>
</html>
"""
    (HERE / "index.html").write_text(page, encoding="utf-8")
    summary = {
        "generated": pd.Timestamp.today().strftime("%Y-%m-%d"), "api_checked": API_CHECKED,
        "rule": {"rp": A.RP_THRESH, "n_gauges": A.N_GAUGES, "months": A.WET_MONTHS, "years": [A.START, A.END]},
        "levels": {k: lv[k].round(1).to_dict() for k in ("old", "new")},
        "years": {"old": old["years"], "new": new["years"]},
        "first_day": {"old": {y: str(x.date()) for y, x in old["first_crossing"].items()},
                      "new": {y: str(x.date()) for y, x in new["first_crossing"].items()}},
        "forecast_years": {"old": old["rf_years"], "new": new["rf_years"]},
        "first_forecast_issue": {"old": {y: str(x[0].date()) for y, x in ffo.items()},
                                 "new": {y: str(x[0].date()) for y, x in ffn.items()}},
        "old_levels_on_new_flows_1998_2023": mismatch,
        "new_2024_2025": {"years": new["ext_years"], "max_gauges": {int(k): int(v) for k, v in new["ext_n_exc"].items()}},
    }
    (HERE / "data.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {HERE / 'index.html'}")
    print("shifts", shifts, "forecast shifts", fshifts, "leads", leads, "peak25", round(peak25, 2))


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
