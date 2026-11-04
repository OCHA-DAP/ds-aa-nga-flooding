
# Small-multiple line chart with a hover crosshair, shared by both comparison
# pages. Series values are a share of each model's own level (1 = the level).
SM_CSS = """
.sm-legend { display:flex; flex-wrap:wrap; gap:18px; margin:14px 0 4px; font-size:13px; color:var(--n8,#3a4552); }
.sm-legend span.sw { display:inline-block; width:18px; height:3px; border-radius:2px; vertical-align:middle; margin-right:7px; }
.sm-legend span.sw.dash { background:none !important; border-top:3px dashed; height:0; }
.sm-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(250px,1fr)); gap:14px 18px; margin:8px 0 6px; }
.sm-panel { background:#fff; border:1px solid #e2e7e7; border-radius:5px; padding:10px 8px 4px; }
.sm-title { font-size:13px; font-weight:700; color:var(--n9,#1a2733); padding-left:4px; }
.sm-note { font-size:11.5px; color:#6b7683; padding-left:4px; min-height:15px; }
.sm-panel svg { width:100%; height:auto; display:block; overflow:visible; }
.sm-panel .g { stroke:#eef1f3; stroke-width:1; }
.sm-panel .ref { stroke:#5c6b7a; stroke-width:1; stroke-dasharray:4 3; }
.sm-panel .yt, .sm-panel .xt { font-size:10px; fill:#6b7683; font-family:Roboto,Arial,sans-serif; }
.sm-panel .yt { text-anchor:end; } .sm-panel .xt { text-anchor:start; }
.sm-panel .xh { stroke:#3a4552; stroke-width:1; }
.sm-panel path.ln { fill:none; stroke-width:2; stroke-linejoin:round; stroke-linecap:round; }
.sm-tip { position:absolute; display:none; pointer-events:none; background:#fff; border:1px solid #d3d9e0;
  border-radius:4px; padding:7px 10px; font-size:12px; color:#1a2733; box-shadow:0 2px 8px rgba(0,0,0,.12);
  z-index:20; line-height:1.55; }
.sm-tip .sw { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:6px; vertical-align:-1px; }
"""

SM_JS = r"""
function drawSM(root, spec) {
  const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const W = 300, H = 168, m = {l: 38, r: 8, t: 8, b: 20};
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  let tip = document.querySelector('.sm-tip');
  if (!tip) { tip = document.createElement('div'); tip.className = 'sm-tip'; document.body.appendChild(tip); }
  const pct = v => Math.round(v * 100) + '%';
  spec.panels.forEach(p => {
    const n = p.n, x0 = Date.parse(p.x0 + 'T00:00:00Z');
    const xs = i => m.l + i * iw / (n - 1);
    const ys = v => m.t + ih - Math.min(Math.max(v, 0), spec.yMax) / spec.yMax * ih;
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${p.title}: ${spec.aria}">`;
    spec.yTicks.forEach(t => {
      s += `<line class="g" x1="${m.l}" x2="${W - m.r}" y1="${ys(t)}" y2="${ys(t)}"/>`;
      s += `<text class="yt" x="${m.l - 5}" y="${ys(t) + 3}">${pct(t)}</text>`;
    });
    s += `<line class="ref" x1="${m.l}" x2="${W - m.r}" y1="${ys(1)}" y2="${ys(1)}"/>`;
    for (let i = 0; i < n; i++) {
      const d = new Date(x0 + i * 864e5);
      if (d.getUTCDate() === 1) {
        s += `<line class="g" x1="${xs(i)}" x2="${xs(i)}" y1="${m.t}" y2="${m.t + ih}"/>`;
        s += `<text class="xt" x="${xs(i) + 3}" y="${H - 5}">${MON[d.getUTCMonth()]}</text>`;
      }
    }
    spec.series.forEach(se => {
      const v = p.data[se.key]; if (!v) return;
      let d = '', pen = false;
      v.forEach((y, i) => {
        if (y === null) { pen = false; return; }
        d += (pen ? 'L' : 'M') + xs(i).toFixed(1) + ',' + ys(y).toFixed(1); pen = true;
      });
      s += `<path class="ln" d="${d}" stroke="${se.color}"${se.dash ? ` stroke-dasharray="${se.dash}"` : ''}/>`;
    });
    (p.marks || []).forEach(mk => {
      const se = spec.series.find(x => x.key === mk.key), v = p.data[mk.key];
      if (!se || !v || v[mk.i] === null) return;
      s += `<circle cx="${xs(mk.i)}" cy="${ys(v[mk.i])}" r="4.5" fill="${se.color}" stroke="#fff" stroke-width="2"/>`;
    });
    s += `<line class="xh" x1="0" x2="0" y1="${m.t}" y2="${m.t + ih}" style="display:none"/>`;
    s += `<rect x="${m.l}" y="${m.t}" width="${iw}" height="${ih}" fill="transparent"/></svg>`;
    const fig = document.createElement('div');
    fig.className = 'sm-panel';
    fig.innerHTML = `<div class="sm-title">${p.title}</div><div class="sm-note">${p.note || ''}</div>` + s;
    root.appendChild(fig);
    const svg = fig.querySelector('svg'), xh = svg.querySelector('.xh');
    svg.addEventListener('mousemove', e => {
      const r = svg.getBoundingClientRect();
      let i = Math.round(((e.clientX - r.left) * W / r.width - m.l) / (iw / (n - 1)));
      i = Math.max(0, Math.min(n - 1, i));
      xh.setAttribute('x1', xs(i)); xh.setAttribute('x2', xs(i)); xh.style.display = '';
      const d = new Date(x0 + i * 864e5);
      let h = `<b>${d.getUTCDate()} ${MON[d.getUTCMonth()]} ${d.getUTCFullYear()}</b>`;
      spec.series.forEach(se => {
        const v = p.data[se.key];
        if (v && v[i] !== null) h += `<div><span class="sw" style="background:${se.color}"></span>${se.label}: ${pct(v[i])}</div>`;
      });
      tip.innerHTML = h; tip.style.display = 'block';
      const tx = e.pageX + 14, tw = tip.offsetWidth;
      tip.style.left = (tx + tw > window.scrollX + document.documentElement.clientWidth ? e.pageX - tw - 14 : tx) + 'px';
      tip.style.top = (e.pageY + 12) + 'px';
    });
    svg.addEventListener('mouseleave', () => { xh.style.display = 'none'; tip.style.display = 'none'; });
  });
}
"""


def sm_panel(title, note, x0, x1, series, marks=None):
    """series: {key: pd.Series indexed by date (share of level)}; marks: {key: date}."""
    import pandas as pd

    days = pd.date_range(x0, x1, freq="D")
    data = {}
    for k, s in series.items():
        if s is None:
            continue
        v = s.reindex(days)
        if v.notna().any():
            data[k] = [None if pd.isna(x) else round(float(x), 3) for x in v]
    mk = []
    for k, d in (marks or {}).items():
        if d is not None and k in data:
            i = (pd.Timestamp(d).normalize() - pd.Timestamp(x0)).days
            if 0 <= i < len(days):
                mk.append({"key": k, "i": int(i)})
    return {"title": title, "note": note, "x0": str(pd.Timestamp(x0).date()), "n": len(days),
            "data": data, "marks": mk}
