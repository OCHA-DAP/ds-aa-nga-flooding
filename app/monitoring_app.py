"""Niger/Benue multi-state flood-trigger monitoring app.

A national map of every forecast station/gauge and the per-state action triggers
derived from the config registry (src.config). Click a state to see its trigger
conditions, full historical firing record, and live Google Flood Hub status.

Run:  streamlit run app/monitoring_app.py

Data is loaded live from blob via ocha-stratus (cached); real-time forecasts come
from the Google Flood Forecasting API (needs GOOGLE_API_KEY).
"""

import altair as alt
import folium
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from shapely.geometry import Point
from streamlit_folium import st_folium

load_dotenv()

import ocha_stratus as stratus  # noqa: E402

from src.config import (  # noqa: E402
    list_presets,
    load_gauge_registry,
    load_lga_registry,
    load_state_params,
    resolve_selection,
)
from src.config.registry import PROJECT_PREFIX  # noqa: E402
from src.datasources import grrr  # noqa: E402

st.set_page_config(page_title="Niger/Benue flood-trigger monitor", layout="wide")

STATE_COLORS = {
    "Adamawa": "#1f77b4", "Taraba": "#ff7f0e", "Benue": "#2ca02c",
    "Nasarawa": "#d62728", "Kogi": "#9467bd", "Kebbi": "#8c564b",
    "Niger": "#e377c2", "Kwara": "#7f7f7f", "Anambra": "#bcbd22",
    "Edo": "#17becf", "Delta": "#aec7e8", "Bayelsa": "#ffbb78",
    "Rivers": "#98df8a", "Imo": "#ff9896",
}
TIER_COLOR = {"strong": "#1a9850", "moderate": "#fdae61",
              "weak": "#d73027", "none": "#cccccc"}

# Correlation bands fitted to the actual best_r distribution (raw daily Spearman
# is seasonally inflated, so the 285 gauges all fall between 0.38–0.70 — bands
# like <0.3/>0.5 would make almost everything green).
CORR_BANDS = [(0.6, "high", "#1a9850"), (0.5, "medium", "#fec44f"),
              (-1.0, "low", "#d73027")]


def corr_band(r):
    if r is None or pd.isna(r):
        return "n/a", "#999999"
    for lo, name, color in CORR_BANDS:
        if r >= lo:
            return name, color
    return "n/a", "#999999"


def viability(f1, fpr):
    if f1 is None or pd.isna(f1):
        return "none"
    if fpr is not None and not pd.isna(fpr) and fpr > 0.2:
        return "weak"
    if f1 >= 0.70:
        return "strong"
    if f1 >= 0.55:
        return "moderate"
    return "weak"


@st.cache_data(show_spinner="Loading registries + boundaries from blob…")
def load_data():
    lga = load_lga_registry()
    gauge = load_gauge_registry()
    sp = load_state_params()
    try:
        perf = stratus.load_parquet_from_blob(
            f"{PROJECT_PREFIX}/processed/config/trigger_performance.parquet")
    except Exception:
        perf = pd.DataFrame()
    try:
        year_detail = stratus.load_parquet_from_blob(
            f"{PROJECT_PREFIX}/processed/config/trigger_year_detail.parquet")
    except Exception:
        year_detail = pd.DataFrame()
    try:
        skill = stratus.load_parquet_from_blob(
            f"{PROJECT_PREFIX}/processed/config/reforecast_skill.parquet")
    except Exception:
        skill = pd.DataFrame()
    # Keep the geospatial footprint small (shared App Service plan is memory
    # tight): state polygons from admin-1, only the riverine LGA polygons from
    # admin-2, both simplified for national-scale display.
    adm1 = stratus.codab.load_codab_from_blob("NGA", admin_level=1)[
        ["ADM1_EN", "geometry"]].copy()
    adm1["geometry"] = adm1.geometry.simplify(0.01)
    adm2 = stratus.codab.load_codab_from_blob("NGA", admin_level=2)
    riverine = adm2[adm2["ADM2_PCODE"].isin(set(lga["pcode"]))][
        ["ADM2_PCODE", "ADM2_EN", "ADM1_EN", "geometry"]].copy()
    riverine["geometry"] = riverine.geometry.simplify(0.005)
    del adm2
    return lga, gauge, sp, perf, year_detail, skill, riverine, adm1


@st.cache_data(ttl=1800, show_spinner="Fetching live Google forecasts…")
def fetch_live(gauge_ids):
    """Return (peak per gauge, current-forecast timeseries, latest issue time)."""
    empty = (pd.DataFrame(), pd.DataFrame(), None)
    if not gauge_ids:
        return empty
    try:
        f = grrr.get_gauge_forecasts(list(gauge_ids))
    except Exception:
        return empty
    if f.empty:
        return empty
    # keep each gauge's most-recently-issued forecast horizon only
    f = f.sort_values("issued_time")
    latest_per_gauge = f.groupby("gauge_id")["issued_time"].transform("max")
    cur = f[f["issued_time"] == latest_per_gauge]
    ts = cur[["gauge_id", "forecast_start", "value"]].copy()
    peak = cur.groupby("gauge_id")["value"].max().rename("forecast_peak").reset_index()
    return peak, ts, f["issued_time"].max()


lga_reg, gauge_reg, state_params, perf, year_detail_all, skill_all, codab_riverine_all, adm1_all = load_data()
sp = state_params.set_index("state")
perf_i = perf.set_index("state") if not perf.empty else pd.DataFrame()

# ── Scope: all riverine states, GRRR + GloFAS candidate gauges ────────────────
sel = resolve_selection("all_riverine", lga_reg, gauge_reg,
                        gauge_sources=("grrr", "glofas"))
scope_states = sorted(sel.lgas["state"].unique())
sel_gauge_ids = set(sel.gauges["gauge_id"])

# ── Header + focus-state picker ───────────────────────────────────────────────
st.title("Niger / Benue flood-trigger monitor")
st.caption(f"{len(scope_states)} riverine states · {len(sel.lgas)} LGAs · "
           f"{sel.gauges['gauge_id'].nunique()} GRRR gauges. Click a state on the "
           "map — or pick one below — for its trigger detail.")

# focus state — plain session key (NOT a widget key) so map-click can set it
if "focus" not in st.session_state or st.session_state.focus not in scope_states:
    st.session_state.focus = scope_states[0]
_pick_col, _ = st.columns([1, 3])
focus = _pick_col.selectbox("Focus state", scope_states,
                            index=scope_states.index(st.session_state.focus))
st.session_state.focus = focus

map_col, detail_col = st.columns([3, 2])

# ── Map ───────────────────────────────────────────────────────────────────────
with map_col:
    codab_riverine = codab_riverine_all[
        codab_riverine_all["ADM2_PCODE"].isin(set(sel.lgas["pcode"]))]
    m = folium.Map(location=[9.0, 8.0], zoom_start=6, tiles="cartodbpositron")

    # states shaded by trigger viability
    adm1_scope = adm1_all[adm1_all["ADM1_EN"].isin(scope_states)].copy()

    def state_style(feat):
        s = feat["properties"]["ADM1_EN"]
        f1 = perf_i.loc[s, "F1"] if s in perf_i.index else None
        fpr = perf_i.loc[s, "FPR"] if s in perf_i.index else None
        c = TIER_COLOR[viability(f1, fpr)]
        return {"fillColor": c, "color": "#555" if s == focus else "#999",
                "weight": 2.5 if s == focus else 0.8, "fillOpacity": 0.30}

    folium.GeoJson(
        adm1_scope, style_function=state_style,
        tooltip=folium.GeoJsonTooltip(fields=["ADM1_EN"], aliases=["State"]),
        name="Trigger viability",
    ).add_to(m)
    folium.GeoJson(
        codab_riverine, name="Riverine LGAs",
        style_function=lambda _: {"fillColor": "#00000000", "color": "#3388ff",
                                  "weight": 0.4, "fillOpacity": 0},
    ).add_to(m)

    # gauges coloured by forecast-skill correlation band; included (selected)
    # trigger gauges get a bold black ring, candidates a thin one. The registry
    # is (gauge, state)-keyed — a cross-river gauge can serve two states — so
    # aggregate to one marker per gauge and list its per-state roles.
    gd = sel.gauges.copy()
    gd["role"] = [f"{s} ★" if b else s
                  for s, b in zip(gd["state"], gd["is_selected"].fillna(False))]
    gdisp = (gd.sort_values("best_r", ascending=False)
             .groupby("gauge_id", as_index=False)
             .agg(lat=("lat", "first"), lon=("lon", "first"),
                  source=("source", "first"), lga_name=("lga_name", "first"),
                  best_r=("best_r", "max"),
                  is_selected=("is_selected", "any"),
                  rp_threshold=("rp_threshold", "first"),
                  roles=("role", ", ".join)))
    # focus-aware styling: the focused state's trigger gauges pop, its other
    # candidates stay normal, everything else fades back
    focus_sel = set(gd[(gd["state"] == focus)
                       & (gd["is_selected"] == True)]["gauge_id"])  # noqa: E712
    focus_cand = set(gd[gd["state"] == focus]["gauge_id"])
    for _, g in gdisp.iterrows():
        is_sel = bool(g["is_selected"])
        is_gf = g["source"] == "glofas"
        r = g["best_r"]
        band, fill = corr_band(r)
        popup = (f"<b>{g['gauge_id']}</b><br>{g['source']} · "
                 f"states: {g['roles']}<br>"
                 f"LGA: {g['lga_name']}<br>best correlation ρ: {r} ({band})<br>"
                 f"RP thresh: {g['rp_threshold']} m³/s<br>"
                 f"{'★ INCLUDED (trigger gauge)' if is_sel else 'candidate (not used)'}")
        gid = g["gauge_id"]
        if gid in focus_sel:
            radius, weight, op, fop = 9, 3.2, 1.0, 1.0
            ring = "#1f6fb4" if is_gf else "#000000"
        elif gid in focus_cand:
            radius = 6 if is_sel else 3.5
            weight, op, fop = (1.6 if is_sel else 0.7), 0.9, 0.75
            ring = "#1f6fb4" if is_gf else ("#333333" if is_sel else "#888888")
        else:
            radius = 5 if is_sel else 3
            weight, op, fop = (1.0 if is_sel else 0.5), 0.35, 0.25
            ring = "#1f6fb4" if is_gf else ("#555555" if is_sel else "#999999")
        folium.CircleMarker(
            [g["lat"], g["lon"]],
            radius=radius, color=ring, weight=weight, opacity=op,
            fill=True, fill_color=fill, fill_opacity=fop,
            popup=popup).add_to(m)

    out = st_folium(m, use_container_width=True, height=620,
                    returned_objects=["last_object_clicked"])
    st.caption("State fill = trigger viability (green strong · amber moderate · red "
               "weak). Gauge **colour = correlation with Floodscan**: "
               "🔴 ρ < 0.5 · 🟡 0.5–0.6 · 🟢 ≥ 0.6 (bands fit the observed 0.38–0.70 "
               "range — raw daily ρ is seasonally inflated). "
               "**Large black-ringed dots are the included trigger gauges**, "
               "small thin dots are unused candidates.")

    # click → focus state (point-in-polygon)
    click = (out or {}).get("last_object_clicked")
    if click:
        pt = Point(click["lng"], click["lat"])
        hit = adm1_all[adm1_all.contains(pt)]
        if len(hit):
            s = hit.iloc[0]["ADM1_EN"]
            if s in scope_states and s != focus:
                st.session_state.focus = s
                st.rerun()

# ── Detail panel ──────────────────────────────────────────────────────────────
with detail_col:
    s = focus
    row = perf_i.loc[s] if s in perf_i.index else None
    cfg = sp.loc[s] if s in sp.index else None
    tier = viability(row["F1"] if row is not None else None,
                     row["FPR"] if row is not None else None)
    badge = {"strong": "🟢 usable", "moderate": "🟡 marginal",
             "weak": "🔴 weak / not recommended", "none": "⚪ no trigger"}[tier]
    st.subheader(f"{s}  ·  {badge}")

    if row is None or cfg is None:
        st.info("No derived trigger for this state (no main-channel GRRR gauges).")
    else:
        gsel = gauge_reg[(gauge_reg["state"] == s)
                         & (gauge_reg["is_selected"] == True)]  # noqa: E712
        n_sel = len(gsel)
        n_req = int(row.get("n_required", 0)) or __import__("math").ceil(
            cfg["consensus_frac"] * n_sel)

        # --- trigger conditions ---
        st.markdown("**Trigger condition**")
        st.markdown(
            f"Fires when **≥ {n_req} of {n_sel}** selected gauges exceed their "
            f"**{int(cfg['rp_target'])}-yr** return-period threshold on the same day "
            f"(aggregated by *{cfg['group_by']}*).")
        c1, c2, c3 = st.columns(3)
        c1.metric("POD", f"{row['POD']:.2f}")
        c2.metric("FPR", f"{row['FPR']:.2f}")
        c3.metric("F1", f"{row['F1']:.2f}")

        # --- live Google status (GRRR gauges only; no GloFAS live feed here) ---
        st.markdown("**Live status — Google Flood Hub**")
        gsel_live = gsel[gsel["source"] == "grrr"]
        peak, ts, issued = fetch_live(tuple(gsel_live["gauge_id"]))
        if peak.empty:
            st.warning("No live forecasts returned for these gauges "
                       "(the forecast API only covers Oct 2023 onward).")
        else:
            live = gsel_live.merge(peak, on="gauge_id", how="left")
            live["exceeds"] = live["forecast_peak"] > live["rp_threshold"]
            n_exc = int(live["exceeds"].sum())
            firing = n_exc >= n_req
            st.markdown(
                f"### {'🚨 TRIGGER MET' if firing else '✅ Below trigger'} "
                f"— {n_exc}/{n_sel} gauges over threshold (need {n_req})")
            st.caption(f"Latest forecast issued {pd.to_datetime(issued):%Y-%m-%d %H:%M} UTC")

            # normalized forecast timeseries: value / gauge threshold (1.0 = at threshold)
            thr = dict(zip(gsel_live["gauge_id"], gsel_live["rp_threshold"]))
            plot = ts.copy()
            plot["pct_of_threshold"] = plot["value"] / plot["gauge_id"].map(thr)
            plot["forecast_start"] = pd.to_datetime(plot["forecast_start"])
            lines = (
                alt.Chart(plot).mark_line(point=True).encode(
                    x=alt.X("forecast_start:T", title="forecast time"),
                    y=alt.Y("pct_of_threshold:Q",
                            title="forecast ÷ threshold",
                            axis=alt.Axis(format="%")),
                    color=alt.Color("gauge_id:N", legend=None),
                    tooltip=["gauge_id", "forecast_start:T", "value:Q",
                             alt.Tooltip("pct_of_threshold:Q", format=".0%")],
                )
            )
            rule = alt.Chart(pd.DataFrame({"y": [1.0]})).mark_rule(
                color="#d73027", strokeDash=[6, 4]).encode(y="y:Q")
            st.altair_chart((lines + rule).properties(height=220),
                            use_container_width=True)
            st.caption("Each line = one selected gauge's live forecast as a share of "
                       "its trigger threshold; the red line is the threshold (100%).")

            show = live[["gauge_id", "forecast_peak", "rp_threshold", "exceeds"]].copy()
            show.columns = ["gauge", "forecast (m³/s)", "threshold", "over?"]
            st.dataframe(show.round(1), hide_index=True, height=180, width="stretch")

        # --- season-by-season record (full table) ---
        st.markdown("**Season-by-season record** (reanalysis "
                    f"{int(cfg['analysis_start_year'])}–{int(cfg['analysis_end_year'])}; "
                    "flood seasons run Apr–Mar, labelled by start year)")
        yd = (year_detail_all[year_detail_all["state"] == s].sort_values("year")
              if "state" in year_detail_all.columns else pd.DataFrame())
        if yd.empty:
            st.info("Year detail not available for this state.")
        else:
            disp = yd[["year", "n_gauges_met", "n_required", "fs_rp",
                       "fired", "is_event"]].copy()
            disp.columns = ["Season", "Gauges met", "Needed", "Floodscan RP (yr)",
                            "Triggered", "4-yr flood"]

            def _row_color(r):
                fired, ev = r["Triggered"], r["4-yr flood"]
                c = ("#d7f0d7" if fired and ev else   # hit
                     "#f7d4cf" if ev else             # missed flood
                     "#fde8c8" if fired else "")      # false alarm
                return [f"background-color:{c}"] * len(r)

            styler = (disp.style.apply(_row_color, axis=1)
                      .format({"Floodscan RP (yr)": "{:.1f}"}))
            st.dataframe(styler, hide_index=True, height=430, width="stretch")
            st.caption(
                f"**Gauges met** = peak number of the {n_sel} selected gauges over "
                f"their {int(cfg['rp_target'])}-yr threshold on any single day that "
                f"season (fires when ≥ Needed). "
                "Green = hit · red = missed flood · amber = false alarm.")

        # --- all candidate gauges (included + excluded) ---
        gall = gauge_reg[(gauge_reg["state"] == s)
                         & (gauge_reg["source"].isin(("grrr", "glofas")))]
        with st.expander(f"All candidate gauges ({len(gall)}) — {n_sel} included"):
            gcols = ["is_selected", "gauge_id", "source", "best_r", "best_lag",
                     "rp_threshold", "basin"]
            gt = gall[gcols].copy()
            if "in_state" in gall.columns:
                gt["cross-river"] = ~gall["in_state"].fillna(True)
            gt = gt.sort_values(["is_selected", "best_r"],
                                ascending=[False, False])
            styler = gt.style.apply(
                lambda r: ["background-color:#d7f0d7" if r["is_selected"] else ""]
                * len(r), axis=1)
            st.dataframe(styler, hide_index=True, height=380, width="stretch")
            st.caption(
                f"Ranked by correlation with the state's Floodscan benchmark; "
                f"the top {n_sel} (green rows) form the trigger — the rest were "
                "assessed and excluded. "
                "**cross-river** gauges sit outside the state's own LGAs "
                "(within a 10 km buffer) but inform its trigger.")

        # --- reforecast vs reanalysis skill ---
        with st.expander("Reforecast skill vs reanalysis (can we trust the "
                         "reanalysis-based history?)"):
            sk = (skill_all[skill_all["state"] == s]
                  if "state" in skill_all.columns else pd.DataFrame())
            if sk.empty:
                st.info("Not computed yet — run "
                        "`pipelines/check_reforecast_skill.py`.")
            else:
                if "source" in sk.columns and (sk["source"] == "glofas").any():
                    gf_sk = sk[sk["source"] == "glofas"]
                    pv_gf = gf_sk.pivot_table(index="gauge_id", columns="leadtime",
                                              values="peak_rank_corr")
                    pv_gf.columns = [f"L{int(c)}d" for c in pv_gf.columns]
                    st.markdown("GloFAS (vs its own lead-1, seasons 2003–2022):")
                    st.dataframe(pv_gf.round(2), width="stretch")
                    sk = sk[sk["source"] == "grrr"]
                pv = sk.pivot_table(index="gauge_id", columns="leadtime",
                                    values="peak_rank_corr")
                pv.columns = [f"lead {int(c)}d" for c in pv.columns]
                st.markdown("Rank correlation of **seasonal peak flows** "
                            "(Google reforecast vs reanalysis, Apr–Mar seasons "
                            "2016–2022):")
                def _corr_cell(v):
                    if pd.isna(v):
                        return ""
                    c = ("#d7f0d7" if v >= 0.9 else
                         "#fdf2cc" if v >= 0.7 else "#f7d4cf")
                    return f"background-color:{c}"

                st.dataframe(pv.round(2).style.map(_corr_cell),
                             width="stretch")
                l7 = sk[sk["leadtime"] == sk["leadtime"].max()]
                summ = l7[["gauge_id", "peak_rank_corr", "rank_exact",
                           "peak_bias", "thr_agree"]].rename(columns={
                               "peak_rank_corr": "rank ρ",
                               "rank_exact": "exact ranking",
                               "peak_bias": "peak bias (rf/ra)",
                               "thr_agree": "same seasons over threshold"})
                st.markdown(f"At the longest lead ({int(sk['leadtime'].max())} d):")
                st.dataframe(summ, hide_index=True, width="stretch")
                st.caption(
                    "1.00 rank ρ = the reforecast orders the flood seasons "
                    "exactly like the reanalysis. **Same seasons over "
                    "threshold** checks the trigger-relevant question: would "
                    "the forecast have crossed this gauge's threshold in the "
                    "same seasons the reanalysis did? Only 7 seasons overlap, "
                    "so treat as indicative.")

# ── Season-peak comparison ────────────────────────────────────────────────────
st.divider()
st.subheader("Season-peak comparison")
have_peaks = ("state" in year_detail_all.columns
              and "fs_peak" in year_detail_all.columns)
if not have_peaks:
    st.info("Season-peak data not yet available (year-detail table on blob "
            "predates the fs_peak column — rerun the trigger grid search).")
else:
    ydp = year_detail_all.dropna(subset=["fs_peak"]).copy()
    ydp["frac_met"] = ydp["n_gauges_met"] / ydp["n_selected"]
    cmp_col1, cmp_col2 = st.columns(2)

    # -- state vs state: Floodscan seasonal peaks --
    with cmp_col1:
        st.markdown("**Floodscan seasonal peak — state vs state**")
        yd_states = sorted(ydp["state"].unique())
        c1, c2 = st.columns(2)
        sx = c1.selectbox("X state", yd_states,
                          index=yd_states.index(focus) if focus in yd_states else 0)
        sy = c2.selectbox("Y state", yd_states,
                          index=min(1, len(yd_states) - 1))
        wide = (ydp[ydp["state"].isin([sx, sy])]
                .pivot_table(index="year", columns="state", values="fs_peak")
                .dropna().reset_index())
        if sx == sy or wide.empty:
            st.info("Pick two different states with overlapping seasons.")
        else:
            ev = ydp[ydp["is_event"]].groupby("year")["state"].apply(set)

            def _ev_label(y):
                s_ = ev.get(y, set()) & {sx, sy}
                if s_ == {sx, sy}:
                    return "4-yr flood in both"
                if s_:
                    return f"4-yr flood in {next(iter(s_))} only"
                return "neither"

            wide["flood"] = wide["year"].map(_ev_label)
            pts = alt.Chart(wide).mark_circle(size=90).encode(
                x=alt.X(f"{sx}:Q", title=f"{sx} seasonal peak SFED"),
                y=alt.Y(f"{sy}:Q", title=f"{sy} seasonal peak SFED"),
                color=alt.Color("flood:N", title="",
                                scale=alt.Scale(domain=["4-yr flood in both",
                                                        f"4-yr flood in {sx} only",
                                                        f"4-yr flood in {sy} only",
                                                        "neither"],
                                                range=["#d73027", "#fc8d59",
                                                       "#fdae61", "#bbbbbb"])),
                tooltip=["year:O", alt.Tooltip(f"{sx}:Q", format=".3f"),
                         alt.Tooltip(f"{sy}:Q", format=".3f")],
            )
            labels = pts.mark_text(dy=-9, fontSize=9).encode(text="year:O")
            st.altair_chart((pts + labels).properties(height=340),
                            use_container_width=True)
            st.caption("One point per flood season (Apr–Mar). Peaks generally "
                       "co-move along the river but their *magnitude* is not "
                       "comparable across states — SFED is a flooded fraction, "
                       "so it scales with local floodplain width.")

    # -- gauges vs Floodscan for the focus state --
    with cmp_col2:
        st.markdown(f"**{focus}: gauges over threshold vs Floodscan peak**")
        ydf = ydp[ydp["state"] == focus]
        if ydf.empty:
            st.info("No season detail for this state.")
        else:
            def _outcome(r):
                if r["fired"] and r["is_event"]:
                    return "hit"
                if r["is_event"]:
                    return "missed flood"
                if r["fired"]:
                    return "false alarm"
                return "quiet season"

            ydf = ydf.assign(outcome=ydf.apply(_outcome, axis=1))
            need_frac = float((ydf["n_required"] / ydf["n_selected"]).iloc[0])
            sc = alt.Chart(ydf).mark_circle(size=90).encode(
                x=alt.X("frac_met:Q", title="max fraction of gauges over "
                        "threshold (same day)", axis=alt.Axis(format="%")),
                y=alt.Y("fs_peak:Q", title="Floodscan seasonal peak SFED"),
                color=alt.Color("outcome:N", title="",
                                scale=alt.Scale(domain=["hit", "missed flood",
                                                        "false alarm",
                                                        "quiet season"],
                                                range=["#1a9850", "#d73027",
                                                       "#fdae61", "#bbbbbb"])),
                tooltip=["year:O", alt.Tooltip("frac_met:Q", format=".0%"),
                         alt.Tooltip("fs_peak:Q", format=".3f"),
                         alt.Tooltip("fs_rp:Q", format=".1f", title="FS RP (yr)")],
            )
            labels = sc.mark_text(dy=-9, fontSize=9).encode(text="year:O")
            rule = alt.Chart(pd.DataFrame({"x": [need_frac]})).mark_rule(
                color="#333", strokeDash=[6, 4]).encode(x="x:Q")
            st.altair_chart((sc + labels + rule).properties(height=340),
                            use_container_width=True)
            st.caption("One point per season; dashed line = consensus fraction "
                       "needed to fire. A clean trigger puts 4-yr floods (top) "
                       "right of the line and quiet seasons left of it.")
