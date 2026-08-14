# Presentation notes — 2025 → 2026 Adamawa trigger revision

Prep notes for internal defense of the trigger design change. Sourced from
`trigger_development.md`, `methodology.md`, dated notes (`2026-05-2*.md`,
`2026-06-18.md`), and direct verification against notebooks `19_model_reanalysis_comparison.ipynb`,
`13_model_performance.ipynb`, `03_forecast_correlation.ipynb`, `04_gauge_selection_assessment.ipynb`.
All numbers below were re-checked against saved notebook outputs before inclusion — flagged
exceptions are noted explicitly.

---

## 1. Key updates, 2025 → 2026 (concise)

- **Action trigger redesigned**: single-station OR logic (GloFAS ≥3,132 m³/s **OR** Google GRRR
  ≥1,195 m³/s, both at Wuroboki/Kangli) → multi-gauge GRRR voting (**≥6 of 10 gauges** simultaneously
  exceed their own 4-yr RP threshold).
- **GloFAS demoted, not dropped**: no longer part of the action trigger's OR logic; moved to a new,
  independent **readiness trigger** (GloFAS reforecast ≤12d lead, or reanalysis, >3,132 m³/s).
- **Design RP target shifted**: 2025 calibrated to ~5.4-yr RP; 2026 calibrated to **4-yr RP**
  Floodscan events (n=6: 1999, 2012, 2015, 2018, 2022, 2023).
- **Same activation frequency preserved by design**: both fire **6 times in 26 years** (RP = 4.5 yr,
  22% annual probability). Only *which* years differ — 2003 drops out (was a 3-yr, not 4-yr, event),
  2018 comes in (a genuine 4-yr event 2025 missed).
- **Detection performance at 4-yr RP improves**: POD 50%→67%, FAR 15%→10%, F1 0.50→0.67.
  3-yr and 5-yr RP performance is unchanged between designs.
- **Gauge selection is now systematic and source-blind**: 42 GRRR candidates + GloFAS ranked together
  by Spearman correlation with Floodscan; top 10 taken regardless of source. GloFAS wasn't excluded
  by rule — it ranked 11th+ (ρ=0.679) below the top-10 cutoff (ρ=0.711–0.742).
- **New capability**: readiness trigger didn't exist in 2025 at all. Fires ~once every 3.5 years,
  ahead of the action trigger, for pre-positioning.
- (Separately) **NHF flash-flood trigger** re-targeted via a NiHSA composite risk score — different
  flood type/funding mechanism, not part of the CERF riverine story above.

---

## 2. What's actually better in 2026

**Spatial robustness over single-station fragility**
10 geographically distributed gauges vote (≥60% threshold) instead of one station carrying the
whole trigger. Illustration: one of the 10 selected gauges (`hybas_1120840690`) is a clear outlier —
a small tributary with a 4-yr RP threshold of 143 m³/s (~8× lower than the other 9, ~1,100 m³/s) —
yet it only contributed to 4 of 6 fire years and never dominates the outcome, because the voting
rule is built to absorb a single anomalous gauge. A 2025-style single-station design has no such
safety margin.

**One internally consistent signal, not an OR of two heterogeneous models**
2025's OR logic meant false alarms from *either* model counted against the trigger — a structural
FAR penalty. 2026's action trigger draws on one data source (GRRR) evaluated the same way at every
gauge, which is more diagnosable and defensible than reconciling two different models' failure modes.

**A genuinely new early-warning layer (readiness trigger)**
This is additive capability, not a redesign of something that existed. It uses GloFAS specifically
where GloFAS is strong — a real multi-day ensemble forecast — rather than folding it into a same-day
observational OR condition it was never well-suited to.

**Rigor of the calibration process itself**
2025: single-station grid search over (GloFAS rank, Google rank) pairs, with lead time used only as
a tiebreaker, not an optimization target (per `2026-05-27.md`).
2026: systematic gauge discovery (42 candidates) → correlation ranking → grid search over
(RP threshold × N-gauges-required) scored on POD/FAR/F1 → explicit small-sample caveats stated
throughout ("each event year shifts POD/F1 by ~17 percentage points at n=6").

**Note on two different "2025 numbers" — don't conflate them**
There are two distinct sets of 2025 figures floating around, and only one of them is the fair
comparison:

- **The headline comparison used above (POD 50%→67%, FAR 15%→10%, F1 0.50→0.67 at 4-yr RP; identical
  at 3-yr/5-yr) is a direct, apples-to-apples recomputation.** Both trigger *definitions* — 2025's
  single-station OR and 2026's 6-of-10 voting — were re-run against the **same** evaluation period
  (1998–2023), the **same** Floodscan SFED dataset and Weibull-derived event years, and the **same**
  scoring code. Only the trigger logic differs. This is exactly the comparison you'd want, and it's
  the one to defend.
- **Separately**, the 2025 framework's own *originally published* FAR figures (18% at 3-yr, 10% at
  5-yr, from the endorsed 2025 PDF) are **confirmed typos in that document** — not evidence of a
  different evaluation methodology. (`notes/2026-06-18.md` had speculated they might reflect a
  different period/dataset/threshold convention; that speculation is superseded now that the typo
  has been confirmed.) Still don't cite those specific old PDF figures next to the 2026 metrics —
  they're simply wrong, not an alternate valid calculation — but there's no methodological
  discrepancy left to explain.

**Early (pre-final-design) evidence that ensembling itself is the source of the gain**
An earlier prototype (`16_google_ensemble_exploration.ipynb`, Google's official RP thresholds, before
the empirical-Weibull/top-10 refinement) already showed multi-gauge aggregation beating single-station
at the 4-yr RP benchmark: POD 0.50→0.67, FAR 0.40→0.20, F1 0.55→0.73. This isolates spatial
aggregation as a real driver of improvement, separate from any later threshold retuning.

---

## 3. Justifying the lead-time change (and why it's not a regression)

### The honest framing

The 2025 headline lead-time numbers (~13.7 days mean for Google, ~24.7 days for GloFAS, to a 5-yr RP
Floodscan threshold) were a by-product of the (RP-rank, RP-rank) grid search that selected the 2025
trigger configuration — lead time was recorded as a **tiebreaker** among candidate configurations,
not as a metric that was itself validated or optimised end-to-end. It's a smaller, narrower slice of
years than a full event-year evaluation, so it's reasonable to want a fuller picture before repeating
it as a headline figure.

The 2026 numbers take a broader, more conservative view:

- measured against **first day Floodscan SFED crosses the 4-yr RP threshold**, across **all** 6
  identified 4-yr RP event years (not a subset) — including the years where the signal misses,
- with explicit small-sample caveats stated alongside every number,
- and — this is the part worth stating plainly — the raw GRRR **reanalysis** signal fires at
  roughly the same time the flood threshold is crossed (0 to −6 days in 2012/2018/2022), *not* weeks
  ahead. The 1999 outlier (+41d) is flagged as atypical, not representative.

So the apparent "reduction" isn't the new design performing worse — it's that 2026 evaluates lead
time the same rigorous way it evaluates everything else (full event-year set, misses included,
uncertainty stated), rather than reporting a number that happened to fall out of a different
selection process.

### Where the +5-day forecast adjustment comes from

Because the GRRR **reforecast** archive is short, the action trigger is calibrated and backtested on
GRRR **reanalysis** (1998–2023) as a historical proxy, then a flat **+5-day** offset is applied to
estimate what the operational reforecast would add. This isn't an arbitrary optimism adjustment — it's
justified directly by GRRR's forecast skill (§ below), which is strong enough at short lead times that
reanalysis is a defensible stand-in for calibration purposes, while the reforecast is what's actually
used operationally.

With the +5-day estimate applied: 2012 → +5d, 2018 → +4d, 2022 → −1d (2022's flood simply rose faster
than a 5-day lead can cover), 1999 → +46d (outlier, not representative).

### The core evidence: Google (GRRR) forecast skill vs GloFAS

This is the part that actually justifies leaning on Google as the primary signal. Three independent,
verified comparisons, all pointing the same direction:

| Comparison | GloFAS | Google GRRR | Source |
|---|---|---|---|
| Best-lag Spearman ρ vs Floodscan SFED (wet season, 1998–2023, n=3,172 days) | **0.679** (lag +1d) | median 0.701 (lag +2d) across 42 candidate gauges; best individual gauge **0.742** (lag −3d) | `19_model_reanalysis_comparison.ipynb` cell 24 / `03_forecast_correlation.ipynb` (identical, independently reproduced) |
| Single-station same-season correlation (Aug–Dec, 1998–2023) | Pearson 0.615 / Spearman 0.748 | Pearson 0.700 / Spearman 0.754 | `13_model_performance.ipynb` cell 10 |
| Classification skill at actual 5-yr RP flood events, single station, endorsed 2025 thresholds | Precision 0.50 / Recall 0.40 / **F1 0.44** | Precision 0.60 / Recall 0.60 / **F1 0.60** | `13_model_performance.ipynb` cell 12 |

All 10 gauges eventually selected into the action trigger ranked above GloFAS (ρ 0.711–0.742 vs
GloFAS's 0.679) — GloFAS wasn't excluded by design, it lost on merit (`04_gauge_selection_assessment.ipynb`
cell 5: "Selected 10 gauges: 10 GRRR, 0 GloFAS — best_r range: 0.711–0.742").

On the classification test specifically: GloFAS misses 2012 and 2015 (2 of 5 actual 5-yr RP events) and
false-alarms in 1998 and 2003; Google misses only 2015, correctly catches 2012, and false-alarms only
in 1998. This is a direct, event-level demonstration that Google's forecast is doing real work GloFAS
isn't — not just a marginally-better correlation coefficient.

**A caveat worth raising proactively** (this is good for credibility in a defense-style Q&A): on
*annual peak-timing* correlation specifically (as opposed to daily correlation), GloFAS (ρ=0.385) is
actually comparable to or better than most of the individual GRRR gauges selected into the top 10
(which cluster at ρ≈0.34–0.36 on this specific metric, despite ranking highest on daily correlation).
The story isn't "GloFAS is worse on every axis" — it's that GRRR's advantage is concentrated in daily
correlation and event-detection skill, which is what the action trigger actually needs, while GloFAS
has some relative strength in peak-timing structure at a single station. Framing it this way is more
defensible than an unqualified "Google is just better."

**Sampling-density caveat** (also worth surfacing, not hiding): GloFAS's reforecast is issued only
twice weekly (~35 issue dates/season vs ~120 for a daily product), so any skill-degradation-with-leadtime
estimate for GloFAS at high-RP conditions rests on much smaller samples (n≤15) than Google's. This
matters two ways: (1) it's a reason to distrust *strong* claims about GloFAS's long-leadtime skill in
either direction, and (2) it's part of *why* the readiness trigger keeps GloFAS's leadtime window
capped at 12 days rather than pushing further out where its sampling gets thin.

*(Two notebooks — `20_trigger_tuning.ipynb` and `14_forecast_performance.ipynb` — appear to have stale
saved outputs from a different state config (Benue, not Adamawa) and were excluded from the numbers
above. Don't cite their printed tables without re-running first.)*

### One-line synthesis for the slide

> "The 2026 lead-time figures look shorter because they're measured across every actual event year,
> not just the subset that fell out of the original selection process. We built the action trigger
> on Google because it objectively correlates better with observed flooding and catches flood years
> GloFAS misses — and we recovered the lead time GloFAS is genuinely good at by giving it its own
> readiness trigger, instead of blending both into one same-day OR condition."

---

## Anticipated questions (defense prep)

**"Is the 2025 vs 2026 performance comparison actually apples-to-apples?"**
Yes, for the numbers that matter (POD 50%→67%, FAR 15%→10%, F1 0.50→0.67 at 4-yr RP; identical at
3-yr/5-yr). Both trigger definitions were re-run against the same 1998–2023 period, same Floodscan
dataset, same Weibull thresholds, same scoring code — only the trigger logic changed. The one thing
*not* to cite side-by-side is the 2025 framework's original *published* FAR figures (18%/10%) —
those have since been confirmed as typos in that document, not a different calculation, so they're
just not reliable numbers to cite at all, from either side.

**"Why 4-yr RP and not 5-yr like 2025?"**
4-yr RP still targets 5-year-RP-level CERF activation frequency, but calibrates against *observed*
Floodscan flooding rather than modelled discharge rank. It also gives a larger event sample (n=6 vs
n=3-ish at 5-yr) for a more stable calibration, while the overall activation frequency (RP≈4.5yr, one
in ~4.5 years) is unchanged from 2025 by explicit design choice.

**"Isn't n=6 event years too small to trust a POD/F1 improvement?"**
Yes, and this is stated explicitly in the doc: each single year shifts POD/F1 by ~17 percentage
points. Treat 50%→67% as indicative, not statistically proven — the honest framing is "no worse, and
directionally better, on a small sample," not "provably better."

**"Why remove GloFAS from the action trigger?"**
It wasn't removed by a rule — it was ranked against the same pool of candidates and came in below the
top-10 cutoff (0.679 vs 0.711–0.742). It's not thrown away — it's redeployed as a readiness trigger,
which is arguably a better fit for what GloFAS's ensemble reforecast is actually good at.

**"Why should we trust the +5-day reforecast estimate over reanalysis?"**
Because GRRR's own forecast skill is very strong at short lead times (this is the whole point of
the skill comparison above) — the assumption is explicitly flagged as an estimate in both the
notebook and the documentation, and the sensitivity is small at the lead times actually used (5
days), not extrapolated out to a long, untested horizon.

**"2015 and 2023 are still missed by both designs — isn't that concerning?"**
Yes — flagged explicitly in the doc's appendix as an open problem, not something the 2026 redesign
claims to have solved. Worth naming directly rather than waiting for someone to find it: possible
causes (different flood pathway, Lagdo dam release timing, gauge coverage gaps) are listed as
unresolved and a stated priority before operational deployment.
