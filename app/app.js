/* Niger/Benue flood-trigger monitor — static GH Pages app.
   Data is pre-baked JSON in data/ (see pipelines/export_app_data.py). */

const TIER = {
  strong:   { color: "#1a9850", label: "🟢 usable" },
  moderate: { color: "#fdae61", label: "🟡 marginal" },
  weak:     { color: "#d73027", label: "🔴 weak / not recommended" },
  none:     { color: "#cccccc", label: "⚪ no trigger" },
};

function viability(f1, fpr) {
  if (f1 == null || isNaN(f1)) return "none";
  if (fpr != null && !isNaN(fpr) && fpr > 0.2) return "weak";
  if (f1 >= 0.7) return "strong";
  if (f1 >= 0.5) return "moderate";   // endorsed Adamawa sits at 0.545
  return "weak";
}

// Bands fitted to the wet-season ρ distribution (n=464, quartiles .65/.70/.74)
function corrColor(r) {
  if (r == null || isNaN(r)) return "#999999";
  if (r >= 0.7) return "#1a9850";
  if (r >= 0.6) return "#fec44f";
  return "#d73027";
}

const fmt = (v, d = 2) => (v == null || isNaN(v)) ? "–" : (+v).toFixed(d);
const esc = s => String(s).replace(/[&<>"]/g,
  c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

let D = null;          // core.json
let G = null;          // geo.json
let stateLayer = null;
let focus = null;
const markerIndex = [];   // {marker, rows, best} per gauge, for focus restyling

const byState = (arr, key = "state") => {
  const m = {};
  (arr || []).forEach(r => { (m[r[key]] = m[r[key]] || []).push(r); });
  return m;
};
let perfBy = {}, cfgBy = {}, gaugesBy = {}, ydBy = {}, skillBy = {};

Promise.all([
  fetch("data/core.json").then(r => r.json()),
  fetch("data/geo.json").then(r => r.json()),
]).then(([core, geo]) => {
  D = core; G = geo;
  perfBy = Object.fromEntries(core.perf.map(r => [r.state, r]));
  cfgBy = Object.fromEntries(core.states.map(r => [r.state, r]));
  gaugesBy = byState(core.gauges);
  ydBy = byState(core.year_detail);
  skillBy = byState(core.skill);
  init();
}).catch(e => {
  document.getElementById("subtitle").textContent =
    "Failed to load data — " + e;
});

function init() {
  const states = Object.keys(cfgBy).sort();
  const nG = new Set(D.gauges.map(g => g.gauge_id)).size;
  document.getElementById("subtitle").textContent =
    `${states.length} riverine states · ${D.lgas.length} LGAs · ${nG} ` +
    `forecast gauges (Google + GloFAS). Click a state or pick one for detail.`;
  const stamp = [`Data generated ${D.generated_at.slice(0, 16)}Z`];
  if (D.live_issued) stamp.push(`latest Google forecast issued ${D.live_issued.slice(0, 16)}Z`);
  stamp.push("Flood seasons run Apr–Mar, labelled by start year.");
  document.getElementById("stamp").textContent = stamp.join(" · ");

  const sel = document.getElementById("state-select");
  sel.innerHTML = states.map(s => `<option>${esc(s)}</option>`).join("");
  sel.onchange = () => selectState(sel.value);

  const cx = document.getElementById("cmp-x"), cy = document.getElementById("cmp-y");
  const ydStates = Object.keys(ydBy).sort();
  cx.innerHTML = cy.innerHTML = ydStates.map(s => `<option>${esc(s)}</option>`).join("");
  cy.selectedIndex = Math.min(1, ydStates.length - 1);
  cx.onchange = cy.onchange = renderStateScatter;

  buildMap();
  selectState(states[0]);
  renderStateScatter();
}

/* ── map ─────────────────────────────────────────────────────────────────── */

function buildMap() {
  const map = L.map("map", { scrollWheelZoom: true }).setView([9.0, 8.0], 6);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    attribution: "&copy; OpenStreetMap &copy; CARTO", subdomains: "abcd",
  }).addTo(map);

  stateLayer = L.geoJSON(G.states, {
    style: f => stateStyle(f.properties.ADM1_EN),
    onEachFeature: (f, l) => {
      l.on("click", () => selectState(f.properties.ADM1_EN));
      l.bindTooltip(f.properties.ADM1_EN, { sticky: true });
    },
  }).addTo(map);

  L.geoJSON(G.lgas, {
    style: { color: "#3388ff", weight: 0.4, fill: false },
    interactive: false,
  }).addTo(map);

  // one marker per gauge; registry rows are (gauge, state)
  const per = {};
  D.gauges.forEach(r => { (per[r.gauge_id] = per[r.gauge_id] || []).push(r); });
  Object.values(per).forEach(rows => {
    const best = rows.reduce((a, b) =>
      (b.best_r ?? -9) > (a.best_r ?? -9) ? b : a);
    const isSel = rows.some(r => r.is_selected);
    const roles = rows.map(r =>
      `${r.state}${r.is_selected ? " ★" : ""}` +
      (r.rp_threshold != null ? ` (thr ${Math.round(r.rp_threshold)})` : "")
    ).join(", ");
    const gf = best.source === "glofas";
    const marker = L.circleMarker([best.lat, best.lon], {
      radius: isSel ? 7 : 3.5,
      color: gf ? "#1f6fb4" : (isSel ? "#000" : "#888"),
      weight: gf ? 2.4 : (isSel ? 2.2 : 0.6),
      fillColor: corrColor(best.best_r),
      fillOpacity: isSel ? 0.95 : 0.55,
    }).bindPopup(
      `<b>${esc(best.gauge_id)}</b><br>${esc(best.source)} · ${esc(roles)}<br>` +
      `LGA: ${esc(best.lga_name ?? "–")}<br>best ρ: ${fmt(best.best_r, 3)}<br>` +
      (isSel ? "★ INCLUDED (trigger gauge)" : "candidate (not used)")
    ).addTo(map);
    markerIndex.push({ marker, rows, best });
  });
  restyleMarkers(focus);
}

function restyleMarkers(s) {
  markerIndex.forEach(({ marker, rows, best }) => {
    const inState = rows.filter(r => r.state === s);
    const selHere = inState.some(r => r.is_selected);
    const selAny = rows.some(r => r.is_selected);
    const gf = best.source === "glofas";
    if (selHere) {           // the focused state's trigger gauges: pop
      marker.setStyle({ radius: 9, weight: 3.2, opacity: 1, fillOpacity: 1,
                        color: gf ? "#1f6fb4" : "#000" });
      marker.bringToFront();
    } else if (inState.length) {   // candidates for this state: normal
      marker.setStyle({ radius: selAny ? 6 : 3.5, weight: selAny ? 1.6 : 0.7,
                        opacity: 0.9, fillOpacity: 0.75,
                        color: gf ? "#1f6fb4" : (selAny ? "#333" : "#888") });
    } else {                 // unrelated to the focused state: fade back
      marker.setStyle({ radius: selAny ? 5 : 3, weight: selAny ? 1 : 0.5,
                        opacity: 0.35, fillOpacity: 0.25,
                        color: gf ? "#1f6fb4" : (selAny ? "#555" : "#999") });
    }
  });
}

function stateStyle(s) {
  const p = perfBy[s];
  const tier = viability(p?.F1, p?.FPR);
  return {
    fillColor: TIER[tier].color, fillOpacity: 0.3,
    color: s === focus ? "#555" : "#999", weight: s === focus ? 2.5 : 0.8,
  };
}

/* ── detail panel ────────────────────────────────────────────────────────── */

function selectState(s) {
  focus = s;
  document.getElementById("state-select").value = s;
  if (stateLayer) stateLayer.setStyle(f => stateStyle(f.properties.ADM1_EN));
  restyleMarkers(s);
  renderDetail(s);
  renderGaugeScatter(s);
}

function renderDetail(s) {
  const p = perfBy[s], cfg = cfgBy[s];
  const tier = viability(p?.F1, p?.FPR);
  const el = document.getElementById("detail");
  let h = `<h2>${esc(s)} <span class="badge" style="background:${TIER[tier].color}">` +
          `${TIER[tier].label}</span></h2>`;

  if (!p || !cfg) {
    el.innerHTML = h + `<p class="note">No derived trigger for this state.</p>`;
    return;
  }
  const sel = (gaugesBy[s] || []).filter(g => g.is_selected);
  const nSel = sel.length;
  const nReq = Math.max(1, Math.ceil(cfg.consensus_frac * nSel));

  h += `<p><b>Trigger condition</b> — fires when <b>≥ ${nReq} of ${nSel}</b> selected ` +
       `gauges exceed their <b>${Math.round(cfg.rp_target)}-yr</b> return-period ` +
       `threshold on the same day.</p>`;
  h += `<div class="metrics">` +
       [["POD", p.POD], ["FPR", p.FPR], ["F1", p.F1]].map(([k, v]) =>
         `<div class="metric"><div class="v">${fmt(v)}</div><div class="k">${k}</div></div>`
       ).join("") + `</div>`;

  h += liveSection(s, sel, nReq);
  h += seasonSection(s, cfg, nSel);
  h += gaugeSection(s, nSel);
  h += skillSection(s);
  el.innerHTML = h;
  drawLiveChart(s, sel);
}

function liveSection(s, sel, nReq) {
  const withLive = sel.filter(g => g.source === "grrr" && D.live[g.gauge_id]);
  let h = `<h3>Live status — Google Flood Hub</h3>`;
  if (!withLive.length) {
    return h + `<p class="note">No live forecasts available for this state's gauges.</p>`;
  }
  const over = withLive.filter(g => D.live[g.gauge_id].peak > g.rp_threshold);
  const firing = over.length >= nReq;
  h += `<div class="status ${firing ? "firing" : "ok"}">` +
       `${firing ? "🚨 TRIGGER MET" : "✅ Below trigger"} — ${over.length}/${sel.length} ` +
       `gauges over threshold (need ${nReq})</div>`;
  h += `<div id="live-chart"></div>` +
       `<p class="caption">Each line = one selected gauge's live forecast as a share of its ` +
       `trigger threshold; the red line is the threshold (100%).</p>`;
  h += `<details><summary>Live forecast peaks (${withLive.length} gauges)</summary>` +
       `<table><tr><th>gauge</th><th class="num">forecast (m³/s)</th>` +
       `<th class="num">threshold</th><th>over?</th></tr>` +
       withLive.map(g => {
         const pk = D.live[g.gauge_id].peak;
         return `<tr><td>${esc(g.gauge_id)}</td><td class="num">${fmt(pk, 1)}</td>` +
                `<td class="num">${fmt(g.rp_threshold, 1)}</td>` +
                `<td>${pk > g.rp_threshold ? "⚠️ yes" : "no"}</td></tr>`;
       }).join("") + `</table></details>`;
  return h;
}

function seasonSection(s, cfg, nSel) {
  const yd = (ydBy[s] || []).slice().sort((a, b) => a.year - b.year);
  if (!yd.length) return "";
  let h = `<h3>Season-by-season record ` +
          `<span class="note">(${Math.round(cfg.analysis_start_year)}–` +
          `${Math.round(cfg.analysis_end_year)}, seasons Apr–Mar)</span></h3>`;
  h += `<div class="scroll"><table><tr><th>Season</th><th class="num">Gauges met</th>` +
       `<th class="num">Needed</th><th class="num">Floodscan RP</th>` +
       `<th>Triggered</th><th>4-yr flood</th></tr>`;
  yd.forEach(r => {
    const cls = r.fired && r.is_event ? "hit" : r.is_event ? "miss" : r.fired ? "fa" : "";
    h += `<tr class="${cls}"><td>${r.year}</td><td class="num">${r.n_gauges_met}</td>` +
         `<td class="num">${r.n_required}</td><td class="num">${fmt(r.fs_rp, 1)}</td>` +
         `<td>${r.fired ? "✔" : ""}</td><td>${r.is_event ? "✔" : ""}</td></tr>`;
  });
  h += `</table></div>` +
       `<p class="caption"><b>Gauges met</b> = peak number of the ${nSel} selected gauges over ` +
       `their threshold on any single day that season. Green = hit · red = missed flood · ` +
       `amber = false alarm.</p>`;
  return h;
}

function gaugeSection(s, nSel) {
  const rows = (gaugesBy[s] || []);
  if (!rows.length) return "";
  const anyCross = rows.some(g => g.in_state === false);
  const sorted = rows.slice().sort((a, b) =>
    (b.is_selected - a.is_selected) || ((b.best_r ?? -9) - (a.best_r ?? -9)));
  let h = `<details><summary>All candidate gauges (${rows.length}) — ` +
          `${nSel} included</summary>` +
          `<div class="scroll"><table><tr><th>included</th><th>gauge</th><th>source</th>` +
          `<th class="num">ρ</th><th class="num">lag</th><th class="num">threshold</th>` +
          `<th>basin</th>` + (anyCross ? "<th>cross-river</th>" : "") + `</tr>`;
  sorted.forEach(g => {
    h += `<tr${g.is_selected ? ' class="hit"' : ""}>` +
         `<td>${g.is_selected ? "★" : ""}</td>` +
         `<td>${esc(g.gauge_id)}</td><td>${esc(g.source)}</td>` +
         `<td class="num">${fmt(g.best_r, 3)}</td><td class="num">${g.best_lag ?? "–"}</td>` +
         `<td class="num">${fmt(g.rp_threshold, 0)}</td><td>${esc(g.basin ?? "–")}</td>` +
         (anyCross ? `<td>${g.in_state === false ? "yes" : ""}</td>` : "") + `</tr>`;
  });
  const cm = cfgBy[s]?.corr_months;
  h += `</table></div>` +
       `<p class="caption">Ranked by Spearman ρ vs the state's Floodscan benchmark on ` +
       `wet-season daily values${cm ? ` (months ${esc(cm)})` : ""}; ` +
       `the top ${nSel} (★, green rows) form the trigger — the rest were assessed ` +
       `and excluded. ρ "–" = insufficient overlapping data.` +
       (anyCross ? ` <b>cross-river</b> gauges sit outside the state's own LGAs ` +
                   `(within a 10 km buffer) but inform its trigger.` : "") + `</p>`;
  return h + `</details>`;
}

function skillSection(s) {
  const sk = skillBy[s] || [];
  if (!sk.length) return "";
  const bySrc = { grrr: sk.filter(r => r.source === "grrr"),
                  glofas: sk.filter(r => r.source === "glofas") };
  let h = `<details><summary>Reforecast skill vs reanalysis</summary>`;
  for (const [src, rows] of Object.entries(bySrc)) {
    if (!rows.length) continue;
    const leads = [...new Set(rows.map(r => r.leadtime))].sort((a, b) => a - b);
    const gs = [...new Set(rows.map(r => r.gauge_id))];
    const cell = {};
    rows.forEach(r => { cell[r.gauge_id + "|" + r.leadtime] = r.peak_rank_corr; });
    h += `<h3>${src === "grrr" ? "Google (vs reanalysis, seasons 2016–2022)"
               : "GloFAS (vs its own lead-1, seasons 2003–2022)"}</h3>`;
    h += `<div class="scroll"><table><tr><th>gauge</th>` +
         leads.map(l => `<th class="num">L${l}d</th>`).join("") + `</tr>`;
    gs.forEach(g => {
      h += `<tr><td>${esc(g)}</td>` + leads.map(l => {
        const v = cell[g + "|" + l];
        const cls = v == null ? "" : v >= 0.9 ? "cell-good" : v >= 0.7 ? "cell-mid" : "cell-bad";
        return `<td class="num ${cls}">${fmt(v, 2)}</td>`;
      }).join("") + `</tr>`;
    });
    h += `</table></div>`;
  }
  h += `<p class="caption">Rank correlation of seasonal peak flows at each lead time. ` +
       `1.00 = the forecast orders the flood seasons exactly like the reference. ` +
       `Small samples — treat as indicative.</p></details>`;
  return h;
}

/* ── SVG charts ──────────────────────────────────────────────────────────── */

function svgOpen(w, h) {
  return `<svg viewBox="0 0 ${w} ${h}" width="100%" style="max-width:${w}px">`;
}

function scales(vals, lo, hi) {
  const mn = Math.min(...vals), mx = Math.max(...vals);
  const pad = (mx - mn || 1) * 0.08;
  const a = mn - pad, b = mx + pad;
  return v => lo + (v - a) / (b - a) * (hi - lo);
}

function axis(w, h, m) {
  return `<g class="axis"><path d="M${m.l},${m.t} V${h - m.b} H${w - m.r}" fill="none"/></g>`;
}

function drawLiveChart(s, sel) {
  const host = document.getElementById("live-chart");
  if (!host) return;
  const series = sel.filter(g => g.source === "grrr" && D.live[g.gauge_id])
    .map(g => ({
      id: g.gauge_id,
      pts: D.live[g.gauge_id].ts.map(p => ({ t: new Date(p.t).getTime(),
                                             y: p.v / g.rp_threshold })),
    })).filter(sr => sr.pts.length > 1);
  if (!series.length) { host.innerHTML = ""; return; }
  const W = 560, H = 220, m = { l: 46, r: 10, t: 10, b: 24 };
  const ts = series.flatMap(sr => sr.pts.map(p => p.t));
  const ys = series.flatMap(sr => sr.pts.map(p => p.y)).concat([1.05]);
  const sx = scales(ts, m.l, W - m.r), sy = scales(ys, H - m.b, m.t);
  let h = svgOpen(W, H) + axis(W, H, m);
  [0.25, 0.5, 0.75, 1.0].forEach(v => {
    if (v > Math.max(...ys)) return;
    h += `<line class="gridline" x1="${m.l}" x2="${W - m.r}" y1="${sy(v)}" y2="${sy(v)}"/>` +
         `<text class="ylab" x="${m.l - 6}" y="${sy(v) + 4}" text-anchor="end">` +
         `${Math.round(v * 100)}%</text>`;
  });
  h += `<line x1="${m.l}" x2="${W - m.r}" y1="${sy(1)}" y2="${sy(1)}" ` +
       `stroke="#d73027" stroke-dasharray="6 4" stroke-width="1.5"/>`;
  series.forEach((sr, i) => {
    const col = `hsl(${(i * 47) % 360} 55% 45%)`;
    const d = sr.pts.map((p, j) => `${j ? "L" : "M"}${sx(p.t).toFixed(1)},${sy(p.y).toFixed(1)}`).join("");
    h += `<path d="${d}" fill="none" stroke="${col}" stroke-width="1.4"><title>${esc(sr.id)}</title></path>`;
  });
  const t0 = new Date(Math.min(...ts)), t1 = new Date(Math.max(...ts));
  h += `<text class="xlab" x="${m.l}" y="${H - 6}">${t0.toISOString().slice(0, 10)}</text>` +
       `<text class="xlab" x="${W - m.r}" y="${H - 6}" text-anchor="end">` +
       `${t1.toISOString().slice(0, 10)}</text></svg>`;
  host.innerHTML = h;
}

function scatter(host, pts, opts) {
  // pts: {x, y, label, color, title}
  const W = 520, H = 340, m = { l: 52, r: 14, t: 12, b: 40 };
  if (!pts.length) { host.innerHTML = `<p class="note">No overlapping data.</p>`; return; }
  const sx = scales(pts.map(p => p.x).concat(opts.xExtra || []), m.l, W - m.r);
  const sy = scales(pts.map(p => p.y), H - m.b, m.t);
  let h = svgOpen(W, H) + axis(W, H, m);
  if (opts.vline != null) {
    h += `<line x1="${sx(opts.vline)}" x2="${sx(opts.vline)}" y1="${m.t}" y2="${H - m.b}" ` +
         `stroke="#333" stroke-dasharray="6 4"/>`;
  }
  pts.forEach(p => {
    h += `<circle cx="${sx(p.x).toFixed(1)}" cy="${sy(p.y).toFixed(1)}" r="5.5" ` +
         `fill="${p.color}" fill-opacity="0.85"><title>${esc(p.title)}</title></circle>` +
         `<text x="${sx(p.x).toFixed(1)}" y="${(sy(p.y) - 8).toFixed(1)}" ` +
         `text-anchor="middle" font-size="9.5">${esc(p.label)}</text>`;
  });
  h += `<text class="xlab" x="${(m.l + W - m.r) / 2}" y="${H - 6}" text-anchor="middle">` +
       `${esc(opts.xlab)}</text>`;
  h += `<text class="ylab" transform="rotate(-90)" x="${-(H - m.b + m.t) / 2}" y="14" ` +
       `text-anchor="middle">${esc(opts.ylab)}</text></svg>`;
  host.innerHTML = h + (opts.legend || "");
}

function renderStateScatter() {
  const sx = document.getElementById("cmp-x").value;
  const sy = document.getElementById("cmp-y").value;
  const host = document.getElementById("scatter-states");
  if (sx === sy) { host.innerHTML = `<p class="note">Pick two different states.</p>`; return; }
  const ax = Object.fromEntries((ydBy[sx] || []).map(r => [r.year, r]));
  const ay = Object.fromEntries((ydBy[sy] || []).map(r => [r.year, r]));
  const cols = { both: "#d73027", x: "#fc8d59", y: "#fdae61", none: "#bbbbbb" };
  const pts = Object.keys(ax).filter(y => ay[y] && ax[y].fs_peak != null && ay[y].fs_peak != null)
    .map(y => {
      const ex = ax[y].is_event, ey = ay[y].is_event;
      const c = ex && ey ? cols.both : ex ? cols.x : ey ? cols.y : cols.none;
      return { x: ax[y].fs_peak, y: ay[y].fs_peak, label: y, color: c,
               title: `${y}: ${sx} ${fmt(ax[y].fs_peak, 3)} / ${sy} ${fmt(ay[y].fs_peak, 3)}` };
    });
  scatter(host, pts, {
    xlab: `${sx} seasonal peak SFED`, ylab: `${sy} seasonal peak SFED`,
    legend: `<div class="legend-row">` +
      `<span><span class="sw" style="background:${cols.both}"></span>4-yr flood in both</span>` +
      `<span><span class="sw" style="background:${cols.x}"></span>${esc(sx)} only</span>` +
      `<span><span class="sw" style="background:${cols.y}"></span>${esc(sy)} only</span>` +
      `<span><span class="sw" style="background:${cols.none}"></span>neither</span></div>`,
  });
}

function renderGaugeScatter(s) {
  document.getElementById("scatter-gauges-title").textContent =
    `${s}: gauges over threshold vs Floodscan peak`;
  const host = document.getElementById("scatter-gauges");
  const yd = (ydBy[s] || []).filter(r => r.fs_peak != null && r.n_selected > 0);
  const cols = { hit: "#1a9850", miss: "#d73027", fa: "#fdae61", quiet: "#bbbbbb" };
  const pts = yd.map(r => {
    const k = r.fired && r.is_event ? "hit" : r.is_event ? "miss" : r.fired ? "fa" : "quiet";
    return { x: r.n_gauges_met / r.n_selected, y: r.fs_peak, label: r.year,
             color: cols[k], title: `${r.year}: ${r.n_gauges_met}/${r.n_selected} gauges, ` +
                                    `FS peak ${fmt(r.fs_peak, 3)} (RP ${fmt(r.fs_rp, 1)})` };
  });
  const vline = yd.length ? yd[0].n_required / yd[0].n_selected : null;
  scatter(host, pts, {
    xlab: "max fraction of gauges over threshold (same day)",
    ylab: "Floodscan seasonal peak SFED", vline, xExtra: [0, 1],
    legend: `<div class="legend-row">` +
      `<span><span class="sw" style="background:${cols.hit}"></span>hit</span>` +
      `<span><span class="sw" style="background:${cols.miss}"></span>missed flood</span>` +
      `<span><span class="sw" style="background:${cols.fa}"></span>false alarm</span>` +
      `<span><span class="sw" style="background:${cols.quiet}"></span>quiet season</span></div>`,
  });
}
