"""Build exploration/2026/cerf/google-model-update/reselected-gauges/index.html.

Variant of the comparison page: the design's gauge selection (steps 01, 03,
04) re-run on the new Google model, then the endorsed rule applied to the
gauges it picks. Numbers from reselect.py; helpers and styling from
build_page.py.

    PYTHONPATH=. .venv/bin/python exploration/2026/cerf/google-model-update/build_reselect_page.py
"""

import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import analysis as A  # noqa: E402
import build_page as B  # noqa: E402
import reselect as R  # noqa: E402
from sm_chart import SM_CSS, SM_JS, sm_panel  # noqa: E402

OUT = HERE / "reselected-gauges"
# validated together (dataviz validate_palette.js, light): old, new endorsed, new re-selected, FloodScan
C_OLD, C_NEW, C_RES, C_FS = "#2a78d6", "#eb6834", "#9b2c7a", "#1baf7a"
YEARS = B.YEARS  # 1998-2023: the years both model versions cover
CFG = [("old_endorsed", "Old model, endorsed gauges", C_OLD),
       ("new_endorsed", "New model, endorsed gauges", C_NEW),
       ("new_reselected", "New model, re-selected gauges", C_RES)]
SHORT = {"old_endorsed": "Old, endorsed", "new_endorsed": "New, endorsed", "new_reselected": "New, re-selected"}
NUMAN_LON = 12.03  # Numan, where the Gongola joins the Benue
TOWNS = [("Yola", 9.21, 12.48), ("Numan", 9.467, 12.033)]


def gauge_map(ranked_old, ranked_new, endorsed, selected):
    """Outlet locations of the candidates; endorsed and re-selected sets marked."""
    W, H, m = 860, 300, 30
    lon0, lon1, lat0, lat1 = 11.40, 13.10, 9.15, 9.65
    sx = lambda lon: m + (lon - lon0) / (lon1 - lon0) * (W - 2 * m)
    sy = lambda lat: H - m - (lat - lat0) / (lat1 - lat0) * (H - 2 * m)
    old = ranked_old.set_index("gauge_id")
    s = f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Map of the 42 candidate gauges: endorsed and re-selected sets">'
    s += f'<rect x="0" y="0" width="{W}" height="{H}" fill="#f7f9fa" rx="4"/>'
    for name, lat, lon in TOWNS:
        s += (f'<rect x="{sx(lon) - 4}" y="{sy(lat) - 4}" width="8" height="8" fill="#3f4748"/>'
              f'<text x="{sx(lon) + 8}" y="{sy(lat) + 14}" class="mt">{name}</text>')
    for _, g in ranked_new[ranked_new.source == "grrr"].iterrows():
        o = old.loc[g.gauge_id]
        tip = (f"{g.gauge_id.removeprefix('hybas_')}: old rank {int(o['rank'])} ({o.best_r:.3f}), "
               f"new rank {int(g['rank'])} ({g.best_r:.3f})")
        x, y = sx(g.longitude), sy(g.latitude)
        if g.gauge_id in selected:
            s += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{C_RES}" stroke="#fff" stroke-width="2"><title>{tip} &#8212; re-selected</title></circle>'
        elif g.gauge_id in endorsed:
            s += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="#fff" stroke="{C_OLD}" stroke-width="2.5"><title>{tip} &#8212; endorsed</title></circle>'
        else:
            s += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="#b8c1c9"><title>{tip}</title></circle>'
    for lon in (11.5, 12.0, 12.5, 13.0):
        s += f'<text x="{sx(lon)}" y="{H - 8}" class="mt" text-anchor="middle">{lon:.1f}&#176;E</text>'
    s += "</svg>"
    return s


def main():
    res = R.run()
    OUT.mkdir(parents=True, exist_ok=True)
    cf = res["configs"]
    fs_ev = res["fs_events"]
    endorsed, selected = res["endorsed"], res["new"]["selected"]
    ro, rn = res["old"]["ranked"], res["new"]["ranked"]
    base = A.run()  # FloodScan reference (timing, chart) as on the comparison page
    fs_first, fs_ratio = base["fs_first"], base["fs_ratio"]
    rf_eval = cf["old_endorsed"]["rf_complete"]  # forecast seasons both archives cover completely
    for k in cf:
        cf[k]["rf_shared"] = [y for y in cf[k]["rf_years"] if y in rf_eval]
        cf[k]["ff_shared"] = {y: v for y, v in cf[k]["rf_first_forecast"].items() if y in rf_eval}

    # ------------------------------------------------------------ facts the text states
    kept = set(selected) & set(endorsed)
    top15 = rn.iloc[:15]
    downstream = [g for g in selected if rn.set_index("gauge_id").loc[g, "longitude"] < NUMAN_LON]
    yr_old, yr_res = set(cf["old_endorsed"]["years"]), set(cf["new_reselected"]["years"])
    assert cf["new_endorsed"]["years"] == cf["old_endorsed"]["years"]
    lost, gained = sorted(yr_old - yr_res), sorted(yr_res - yr_old)
    assert lost == [2019] and gained == [2020], (lost, gained)
    assert 2019 in fs_ev[3] and 2019 not in fs_ev[4] and 2020 not in fs_ev[3]
    assert all(cf["new_reselected"]["scores"][4][k] == cf["old_endorsed"]["scores"][4][k] for k in ("TP", "FP", "FN"))
    assert (rn.iloc[:10].best_lag == 0).all(), "the text says no time offset at every selected gauge"
    end_ranks = rn.set_index("gauge_id").loc[endorsed, "rank"]

    # ------------------------------------------------------------ year grid
    rows = []
    cells = {}
    fs_years = set(fs_ratio.index.year)
    for y in YEARS:
        top = max([rp for rp in A.FS_RP_LEVELS if y in fs_ev[rp]], default=None)
        cells[y] = ((str(top), "sev" if top >= 4 else "fl", f"{y}: FloodScan 1-in-{top} flood year or rarer")
                    if top else ("", "no", f"{y}: below the FloodScan 1-in-3 level")) if y in fs_years else ("", "nd", "")
    rows.append(("FloodScan flood year", cells))
    for k, lab, _ in CFG:
        cells = {}
        for y, n in cf[k]["n_exc"].items():
            n, act = int(n), int(n) >= A.N_GAUGES
            cells[int(y)] = (str(n), "act" if act else ("z" if n == 0 else "part"),
                             f"{y}: up to {n} of 10 gauges over their 1-in-4 level on one day" + (" (rule met)" if act else ""))
        rows.append((SHORT[k], cells))
    for k, lab, _ in CFG:
        cells = {}
        for y in rf_eval:
            ff = cf[k]["ff_shared"]
            cells[y] = (("&#9679;", "act", f"{y}: a forecast issued {B.d(ff[y][0])} met the rule") if y in ff
                        else ("", "z", f"{y}: no forecast at leads 0-7 met the rule"))
        rows.append((SHORT[k] + ": forecast", cells))
    year_grid = B.grid(rows, "Adamawa action trigger by year, three set-ups")

    # ------------------------------------------------------------ dates
    t_years = sorted(set().union(*(set(cf[k]["first_crossing"]) | set(cf[k]["ff_shared"]) for k, _, _ in CFG)))
    t_rows = ""
    for y in t_years:
        rc = [cf[k]["first_crossing"].get(y) for k, _, _ in CFG]
        fc = [cf[k]["ff_shared"].get(y, (None,))[0] for k, _, _ in CFG]
        top = max([rp for rp in A.FS_RP_LEVELS if y in fs_ev[rp]], default=None)
        t_rows += (f"<tr><td><b>{y}</b></td>" + "".join(f"<td class=n>{B.d(x)}</td>" for x in rc)
                   + "".join(f"<td class=n>{B.d(x)}</td>" for x in fc)
                   + f"<td class=n>{B.d(fs_first.get(y))}</td><td>{'1-in-' + str(top) if top else 'below 1-in-3'}</td></tr>")
    shift = {y: B.change(cf["old_endorsed"]["first_crossing"][y], cf["new_reselected"]["first_crossing"][y])
             for y in sorted(yr_old & yr_res)}

    # ------------------------------------------------------------ chart
    panels = []
    for y in t_years:
        marks = {k.split("_")[0][0] + k.split("_")[1][0]: cf[k]["first_crossing"].get(y) for k, _, _ in CFG}
        note = " &middot; ".join(f"{s} {B.d(cf[k]['first_crossing'].get(y))}".replace("&ndash;", "no")
                                 for (k, _, _), s in zip(CFG, ("old", "new", "re-sel.")))
        panels.append(sm_panel(str(y), note, f"{y}-08-01", f"{y}-11-30",
                               {"oe": cf["old_endorsed"]["margin"], "ne": cf["new_endorsed"]["margin"],
                                "nr": cf["new_reselected"]["margin"], "fs": fs_ratio}, marks=marks))
    spec = {"yMax": 2.0, "yTicks": [0, 0.5, 1, 1.5, 2], "aria": "share of the 1-in-4 level, three set-ups and FloodScan",
            "series": [{"key": "oe", "label": "Old, endorsed", "color": C_OLD},
                       {"key": "ne", "label": "New, endorsed", "color": C_NEW},
                       {"key": "nr", "label": "New, re-selected", "color": C_RES},
                       {"key": "fs", "label": "FloodScan", "color": C_FS, "dash": "4 3"}],
            "panels": panels}

    # ------------------------------------------------------------ ranking table
    o_idx = ro.set_index("gauge_id")
    rk_rows = ""
    for _, g in rn.iterrows():
        o = o_idx.loc[g.gauge_id]
        mark = ("<b style='color:%s'>re-selected</b>" % C_RES if g.gauge_id in selected else
                ("<b style='color:%s'>endorsed</b>" % C_OLD if g.gauge_id in endorsed else ""))
        name = "GloFAS Wuroboki" if g.source == "glofas" else g.gauge_id.removeprefix("hybas_")
        rk_rows += (f"<tr><td class=n>{int(g['rank'])}</td><td>{name}</td><td class=n>{g.latitude:.3f}, {g.longitude:.3f}</td>"
                    f"<td class=n>{g.best_r:.3f} ({B.lag(int(g.best_lag))})</td><td class=n>{int(o['rank'])}</td>"
                    f"<td class=n>{o.best_r:.3f} ({B.lag(int(o.best_lag))})</td><td>{mark}</td></tr>")

    # ------------------------------------------------------------ scores, levels, sweep
    def sc(s):
        f = lambda v: "&ndash;" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.2f}"
        return f"<td class=n>{s['TP']}</td><td class=n>{s['FP']}</td><td class=n>{s['FN']}</td><td class=n>{f(s['POD'])}</td><td class=n>{f(s['FAR'])}</td>"
    s_rows = ""
    for rp in A.FS_RP_LEVELS:
        for k, lab, _ in CFG:
            s_rows += f"<tr><td>1-in-{rp}</td><td>record</td><td>{lab}</td>{sc(cf[k]['scores'][rp])}</tr>"
    for rp in A.FS_RP_LEVELS:
        for k, lab, _ in CFG:
            s_rows += f"<tr><td>1-in-{rp}</td><td>forecast</td><td>{lab}</td>{sc(A.score(cf[k]['rf_shared'], fs_ev[rp], rf_eval))}</tr>"
    lv_rows = "".join(
        f"<tr><td>{g.removeprefix('hybas_')}</td><td class=n>{rn.set_index('gauge_id').loc[g, 'latitude']:.3f}, "
        f"{rn.set_index('gauge_id').loc[g, 'longitude']:.3f}</td><td class=n><em>{v:,.1f}</em></td></tr>"
        for g, v in cf["new_reselected"]["thr"].items())
    sweep = "".join(f"<tr{' class=pick' if rp == A.RP_THRESH else ''}><td>1-in-{rp}</td>"
                    + "".join(f"<td>{B.yrs(cf[k]['sweep'][rp])}</td>" for k, _, _ in CFG) + "</tr>" for rp in (3, 4, 5, 6))

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>New Google model: gauges re-selected &mdash; Nigeria flooding</title>
<meta name="description" content="Variant of the new-Google-model comparison for the Adamawa action trigger: the design's gauge selection re-run on the new model, then the endorsed rule applied to the gauges it picks.">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Merriweather:wght@700&family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
<style>
{B.STYLE}
.map svg {{ width:100%; height:auto; display:block; }} .map .mt {{ font-size:12px; fill:#3f4748; font-family:Roboto,Arial,sans-serif; }}
.key {{ display:flex; flex-wrap:wrap; gap:18px; font-size:13px; color:var(--n8); margin:8px 0 4px; }}
.key i {{ display:inline-block; width:12px; height:12px; border-radius:50%; vertical-align:-1px; margin-right:6px; }}
{SM_CSS}
</style>
<script>{SM_JS}</script>
</head>
<body>
<div class="wrap">
  <header class="hero">
    <p class="crumb"><a href="../../../../../">Nigeria flooding &mdash; anticipatory action</a> / <a href="../">new Google model</a> / variant</p>
    <h1>New Google model: re-selecting the gauges</h1>
    <p>The <a href="../" style="color:#fff">comparison page</a> keeps the ten endorsed gauges and refits only their levels. Here the
       design's gauge selection is re-run on the new model as well, exactly as it was run on the old one, and the endorsed rule
       (at least 6 of 10 gauges over their own 1-in-4 level on the same day) is applied to the ten gauges it picks. All comparisons
       use the years both model versions cover: the record 1998&ndash;2023 and forecasts 2016&ndash;2022. {B.API_CHECKED}.</p>
  </header>

  <article>
    <div class="stats">
      <div class="stat flag"><div class="v">{len(kept)} of 10</div><div class="l">endorsed gauges re-selected on the new model</div></div>
      <div class="stat"><div class="v">{top15.best_r.min():.3f}&ndash;{top15.best_r.max():.3f}</div><div class="l">correlation of the top 15 candidates on the new model: almost a tie</div></div>
      <div class="stat"><div class="v">{', '.join(map(str, lost))} &rarr; {', '.join(map(str, gained))}</div><div class="l">the one activation year that changes, 1998&ndash;2023</div></div>
      <div class="stat"><div class="v">{len(yr_res)} years</div><div class="l">activations 1998&ndash;2023 ({B.rp_text(len(yr_res))}), as before</div></div>
    </div>

    <h2><span class="num">1</span>What changes</h2>
    <ul>
      <li><strong>The gauges.</strong> {'None' if not kept else len(kept)} of the ten endorsed gauges make the new top ten. The new ten sit further down the
          Benue: {len(downstream)} downstream of Numan, where the Gongola joins, and {10 - len(downstream)} between Numan and Yola. The endorsed
          gauges, around Yola and upstream, rank {int(end_ranks.min())} to {int(end_ranks.max())} on the new model.</li>
      <li><strong>The ranking is almost flat.</strong> On the new model the top 15 candidates all correlate with FloodScan between
          {top15.best_r.min():.3f} and {top15.best_r.max():.3f}, so which ten are picked is decided in the third decimal. On the old model the
          top ten ranged from {ro.iloc[:10].best_r.min():.3f} to {ro.iloc[:10].best_r.max():.3f}. The new model also matches FloodScan best with no
          time offset at every one of these gauges.</li>
      <li><strong>Activation years.</strong> {B.yrs(yr_res)} with the re-selected gauges, against {B.yrs(yr_old)} with the endorsed gauges
          (the same on either model). 2019, a FloodScan 1-in-3 year, drops out; 2020, below the FloodScan 1-in-3 level, comes in. Against the
          design's 1-in-4 benchmark the scores are unchanged; against 1-in-3 there is one hit fewer and one false alarm more.</li>
      <li><strong>Timing.</strong> First day the rule is met, re-selected gauges against the old design:
          {", ".join(f"{s} in {y}" for y, s in shift.items())}.</li>
      <li><strong>Forecasts</strong> (complete seasons {rf_eval[0]}&ndash;{rf_eval[-1]}): the re-selected gauges meet the rule in
          {B.yrs(cf['new_reselected']['rf_shared'])}, against {B.yrs(cf['old_endorsed']['rf_shared'])} on the old design and
          {B.yrs(cf['new_endorsed']['rf_shared'])} on the new model with the endorsed gauges.</li>
      <li><strong>Levels.</strong> The re-selected gauges carry more water: 1-in-4 levels of {cf['new_reselected']['thr'].min():,.0f}&ndash;{cf['new_reselected']['thr'].max():,.0f} m³/s,
          against {cf['new_endorsed']['thr'].drop('hybas_1120840690').min():,.0f}&ndash;{cf['new_endorsed']['thr'].max():,.0f} for the endorsed main-channel
          gauges on the new model. The small tributary gauge of the endorsed set drops out.</li>
    </ul>
    <div class="callout">Only the gauge selection is re-run. The return period (1-in-4), the gauge count (6 of 10) and the lead window were
      chosen on the old model by the design's trigger assessment, which is not re-run here. The candidate pool is the one the design
      drew from: the 42 Google gauges Flood Hub listed in the seven target LGAs, plus GloFAS at Wuroboki. When Google switches Flood Hub
      to the new model, its list of gauges may change too.</div>

    <h2><span class="num">2</span>Which gauges</h2>
    <p>Outlets of the 42 candidates (hover for ranks). Filled: the ten re-selected on the new model. Open rings: the ten endorsed gauges.</p>
    <div class="key"><span><i style="background:{C_RES}"></i>re-selected (new model)</span><span><i style="border:2.5px solid {C_OLD};width:9px;height:9px"></i>endorsed (old model)</span><span><i style="background:#b8c1c9"></i>other candidates</span></div>
    <div class="map">{gauge_map(ro, rn, endorsed, selected)}</div>
    <details><summary>Full ranking: all 43 candidates</summary>
    <div class="tablewrap"><table class="data"><thead><tr><th>New rank</th><th>Gauge (HydroBASINS)</th><th>Outlet</th><th>New correlation (lag)</th>
      <th>Old rank</th><th>Old correlation (lag)</th><th></th></tr></thead><tbody>{rk_rows}</tbody></table></div></details>

    <h2><span class="num">3</span>When the trigger is met</h2>
    <p>Rows as on the comparison page: FloodScan class, then for each set-up the most gauges over their own 1-in-4 level on any one day
       (filled where 6 or more), then forecast years (complete seasons {rf_eval[0]}&ndash;{rf_eval[-1]}, leads 0&ndash;7). Hover a cell for details.</p>
    {year_grid}
    <div class="tablewrap"><table class="data"><thead>
      <tr><th rowspan=2>Year</th><th class=grp colspan=3>First day the record meets the rule</th><th class=grp colspan=3>First forecast issue meeting it</th><th rowspan=2>FloodScan over 1-in-4</th><th rowspan=2>FloodScan year</th></tr>
      <tr><th>Old, endorsed</th><th>New, endorsed</th><th>New, re-selected</th><th>Old, endorsed</th><th>New, endorsed</th><th>New, re-selected</th></tr></thead>
      <tbody>{t_rows}</tbody></table></div>
    <div class="sm-legend"><span><span class="sw" style="background:{C_OLD}"></span>Old model, endorsed gauges</span>
      <span><span class="sw" style="background:{C_NEW}"></span>New model, endorsed gauges</span>
      <span><span class="sw" style="background:{C_RES}"></span>New model, re-selected gauges</span>
      <span><span class="sw dash" style="border-color:{C_FS}"></span>FloodScan</span>
      <span><span class="sw dash" style="border-color:#5c6b7a"></span>the 1-in-4 level (100%)</span></div>
    <div class="sm-grid" id="sm-res"></div>
    <p class="figcap">Daily flow at the sixth-highest of each set-up's ten gauges as a share of that gauge's own 1-in-4 level, August to November;
       the rule is met above 100%, and dots mark the first such day. FloodScan: flooded fraction in Adamawa as a share of its own 1-in-4 level.
       Years shown: every year from 1998 to 2023 in which any set-up's record or forecast met the rule.</p>
    <script>drawSM(document.getElementById('sm-res'), {json.dumps(spec)});</script>

    <h2><span class="num">4</span>Performance against FloodScan</h2>
    <p>Scored as in the design. Record: 1998&ndash;2023. Forecast: complete seasons {rf_eval[0]}&ndash;{rf_eval[-1]}.</p>
    <div class="tablewrap"><table class="data"><thead><tr><th>FloodScan level</th><th>Series</th><th>Set-up</th><th>Hits</th><th>False</th><th>Missed</th><th>POD</th><th>FAR</th></tr></thead>
      <tbody>{s_rows}</tbody></table></div>
    <details><summary>Other return periods, same 6-of-10 rule</summary>
    <div class="tablewrap"><table class="data"><thead><tr><th>Level</th><th>Old, endorsed</th><th>New, endorsed</th><th>New, re-selected</th></tr></thead>
      <tbody>{sweep}</tbody></table></div></details>

    <h2><span class="num">5</span>Levels of the re-selected gauges</h2>
    <p>1-in-4 level on the new model's August&ndash;November maxima 1998&ndash;2023 (m³/s).</p>
    <div class="tablewrap"><table class="data"><thead><tr><th>Gauge (HydroBASINS)</th><th>Outlet</th><th>1-in-4 level</th></tr></thead><tbody>{lv_rows}</tbody></table></div>

    <h2><span class="num">6</span>Method</h2>
    <ul>
      <li>Selection as in the design's workflow notebooks 01, 03 and 04: the stored candidate pool; each Google candidate's best Spearman
          correlation with FloodScan (August&ndash;November 1998&ndash;2023, lags of &minus;3 to +14 days); all candidates ranked together, top ten kept.
          GloFAS at Wuroboki keeps its stored correlation ({ro[ro.source == 'glofas'].best_r.iloc[0]:.3f}), as its series does not depend on the Google model.</li>
      <li>Run on the old model, the same code reproduces the stored correlations of all 42 Google candidates exactly and selects the ten endorsed gauges.</li>
      <li>Trigger and scores with the comparison page's code (<code>analysis.py</code>); this variant in <code>reselect.py</code>.</li>
    </ul>
  </article>

  <p class="provenance">Generated {pd.Timestamp.today().day} {pd.Timestamp.today().strftime('%B %Y')} by
    <code>exploration/2026/cerf/google-model-update/build_reselect_page.py</code> in
    <a href="https://github.com/OCHA-DAP/ds-aa-nga-flooding">ds-aa-nga-flooding</a>. Back to the <a href="../">comparison page</a>.</p>
</div>
</body>
</html>
"""
    (OUT / "index.html").write_text(page, encoding="utf-8")
    summary = {
        "generated": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "selected_new": selected, "endorsed": endorsed,
        "ranking_new": rn[["rank", "gauge_id", "source", "best_r", "best_lag"]].round(4).to_dict(orient="records"),
        "levels_reselected": cf["new_reselected"]["thr"].round(1).to_dict(),
        "years": {k: cf[k]["years"] for k, _, _ in CFG},
        "forecast_years_2016_2022": {k: cf[k]["rf_shared"] for k, _, _ in CFG},
        "first_day": {k: {y: str(x.date()) for y, x in cf[k]["first_crossing"].items()} for k, _, _ in CFG},
    }
    (OUT / "data.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {OUT / 'index.html'}")
    print("downstream", len(downstream), "endorsed ranks", end_ranks.to_dict(), "shift", shift)


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
