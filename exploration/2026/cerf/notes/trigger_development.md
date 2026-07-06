# 2026 Trigger Development — Adamawa

[← Index](index.html)

---

## Contents

1. [Background](#background)
2. [Proposed trigger — summary](#proposed-trigger--summary)
3. [Data and methods](#data-and-methods)
4. [Action trigger](#action-trigger)
5. [Readiness trigger](#readiness-trigger)
6. [Performance evaluation](#performance-evaluation)
7. [Trigger specifications](#trigger-specifications)
8. [NHF flash flood trigger](#nhf-flash-flood-trigger)
9. [Appendix](#appendix)

---

## Background

The 2025 Adamawa trigger was a single-station OR trigger: it fired if either GloFAS reanalysis (≥ 3,132 m³/s) or Google GRRR reanalysis (≥ 1,195 m³/s) exceeded its threshold at Wuroboki on any wet-season day. Both thresholds were calibrated to approximately 5.4-year empirical RP and evaluated against Floodscan 5-year RP events.

The 2026 work revisited this design with two goals: to evaluate whether a spatially distributed, multi-gauge action trigger would be more robust than a single-station approach, and to develop an independent GloFAS reforecast readiness trigger that could provide advance warning ahead of the action trigger.

---

## Proposed trigger — summary

| | Action trigger | Readiness trigger |
|---|---|---|
| **Data source** | Google GRRR reanalysis (calibration)† / reforecast (operational) | GloFAS ensemble reforecast |
| **Monitoring point** | 10 gauges on the Benue, Adamawa | Wuroboki (G5004) |
| **Condition** | ≥ 6 of 10 gauges simultaneously exceed their individual 4-yr empirical RP threshold | Reforecast ensemble mean > 3,132 m³/s at LT ≤ 13d, **or** reanalysis > 3,132 m³/s (LT = 0) |
| **Threshold** | ~1,100–1,117 m³/s per gauge (empirical Weibull 4-yr RP from 26-yr reanalysis) | 3,132 m³/s (~5-yr GloFAS RP) |
| **Return period** | ~4.5 years (22% annual probability) | ~3 years (33% annual probability) |
| **Lead time** | To 4-yr RP exceedance: −1 to +5d est. typical (reanalysis date − 5d); +46d in 1999 outlier | To est. action trigger: −3 to +10d (2 TP years; excludes 2019 outlier at +41d) |
| **Historical fire years** | 1998, 1999, 2012, 2018, 2019, 2022 (GRRR reanalysis†) | 2003, 2008, 2012, 2014, 2016, 2019, 2022 (GloFAS reforecast + reanalysis) |
| **Detection rate at 4-yr RP** | 67% (4 of 6 events) | 75% of action years in 2003–2022 (3 of 4) |

*All performance metrics are indicative given small sample sizes (n < 10 events). See [Performance summary](#performance-summary) for full detail.*

*† The action trigger is calibrated and backtested using GRRR reanalysis (1998–2023) rather than reforecast because (a) the GRRR reforecast archive covers a shorter period and (b) the GRRR reanalysis has such strong forecast skill that reanalysis is a valid historical proxy for calibration purposes. The reforecast is used for operational activation.*

---

## Data and methods

### Flood ground truth

All trigger performance evaluation is grounded in Floodscan SFED (Surface Fraction Exposed to Flooding), aggregated as the daily mean across all pixels within a 10 km buffer of the Benue river in Adamawa state. Wet-season (Aug–Nov) annual maxima from 1998–2023 are ranked using the Weibull plotting position formula (RP = (n+1)/k) to identify flood event years at 3-, 4-, and 5-year return periods.

The **4-year RP** is used as the primary design target throughout, giving the following event years: **1999, 2012, 2015, 2018, 2022, 2023** (n = 26, k = 6, RP = 4.5 yr). This targets 5-year RP CERF-level events while calibrating against observed satellite flooding rather than modelled discharge.

### Gauge discovery and ranking

Candidate streamflow gauges were identified by spatial intersection: all Google GRRR gauges whose catchment areas overlap the target LGAs in Adamawa state. This yielded 42 GRRR gauges plus the GloFAS reference point at Wuroboki.

Each gauge was scored by the Spearman rank correlation between its wet-season daily streamflow and the Floodscan SFED time series, optimised over a lag range of −3 to +14 days. A negative lag means the Floodscan signal leads the gauge (flooding observed before the streamflow signal arrives); a positive lag means the gauge leads. Correlation was computed on raw daily values to preserve the shared seasonal cycle. GRRR gauges consistently outperformed GloFAS in daily correlation (best ρ ≈ 0.74 vs 0.68).

The correlation profiles did not show a sharp peak at a single lag — instead there was a gradual, low rise across many lag values, indicating meaningful uncertainty in the best-lag estimate. Many of the top GRRR gauges recorded a best lag of −3 days, which may suggest a slight delay in the model signal relative to observed flooding, though the flat profile means this should be interpreted cautiously rather than taken as a robust timing estimate.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Figure: Correlation by lag (median across all Google gauges, best gauge, and GloFAS)</summary>

![Spearman ρ vs lag for all 42 Google gauges (median ± IQR), the best individual gauge (hybas_1120842990), and GloFAS at Wuroboki. The profile is flat across the full −3 to +14 day range with no clear peak, confirming the uncertainty in best-lag estimates.](../workflow/figures/adamawa/03_corr_lag_profiles.png)

</details>

<details>
<summary>Figure: Gauge correlation ranking (Spearman ρ vs Floodscan SFED)</summary>

![Best Spearman ρ per gauge (all 42 candidates + GloFAS), ranked by correlation with Floodscan SFED. GRRR gauges cluster at the top; the GloFAS point at Wuroboki sits mid-table.](../workflow/figures/adamawa/03_corr_dot_plot.png)

</details>
<!-- markdownlint-enable MD033 -->

---

## Action trigger

### Gauge selection

The top 10 gauges by Spearman ρ were retained from the combined GRRR and GloFAS candidate pool — the selection is purely performance-based with no source-level filtering. In Adamawa, all 10 happen to be GRRR gauges: the GloFAS station at Wuroboki ranked just below the cutoff (ρ ≈ 0.68) against the top GRRR gauges (ρ ≈ 0.71–0.74). Nine of the ten are tightly clustered around 9.3–9.4°N, 12.3–12.9°E on the main Benue channel (best ρ ≈ 0.71–0.74, best lag = −3 days). The tenth, hybas_1120840690, is a clear outlier: its 4-year empirical RP threshold is 143 m³/s — roughly 8× lower than the others (~1,100 m³/s) — it has a positive best lag (Floodscan leads the gauge), and it contributed to only 4 of the 6 action trigger years. It likely represents a small tributary rather than the main channel. It was retained in the gauge pool because a single anomalous gauge cannot dominate the ≥60% voting rule, but it warrants attention in any future gauge set review.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Figure: Map of the 10 selected gauges</summary>

![Map of the 10 selected gauges along the Benue channel in Adamawa](../workflow/figures/adamawa/04_selected_gauges_map.png){width=100%}

</details>
<!-- markdownlint-enable MD033 -->

### Trigger design

A grid search was run over two parameters: the per-gauge return period threshold (2–5 yr) and the minimum number of gauges required to fire simultaneously on the same wet-season day (2–10). For each combination, trigger fire years were counted and scored against Floodscan event years using POD, FPR, and F1.

The selected configuration is: **≥ 6 of 10 gauges simultaneously exceed their individual 4-year empirical Weibull RP threshold on at least one wet-season day.** Empirical 4-year thresholds range from 1,101 to 1,117 m³/s across the main-channel gauges (and 143 m³/s for the outlier). The ≥6/10 requirement (60%) balances spatial corroboration against sensitivity: lower fractions pick up isolated gauge noise, higher fractions miss events where flooding is spatially concentrated.

The ≥8/10 configuration achieved a higher F1 score in the grid search but was not selected. Requiring 8 gauges fires fewer times historically, which raises the overall framework return period and would reduce activation frequency relative to the 2025 trigger. Maintaining **6 fire years in 26** (RP ≈ 4.5 yr) was an explicit design constraint to keep the 2026 trigger consistent with the intervention frequency endorsed in the 2025 framework.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Figure: Grid search results (POD/FAR/F1 by gauge RP threshold and count)</summary>

![Grid search results evaluated against Floodscan 4-yr RP events. Each cell shows POD, FAR (FP/(FP+TN)), and F1 for a given (gauge RP threshold, N gauges required) combination. The selected configuration — 4-yr threshold, 6 gauges — achieves F1 = 0.67 and FAR = 10%.](../workflow/figures/adamawa/05_trigger_grid_fs4yr.png)

</details>
<!-- markdownlint-enable MD033 -->

Over the full reanalysis period 1998–2023, the trigger fires in: **1998, 1999, 2012, 2018, 2019, 2022** — giving a combined Weibull RP of **4.5 years** (n = 26, k = 6), well-matched to the 4-year RP design target.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Figure: Per-gauge contributions across the six trigger fire years</summary>

![Per-gauge contributions across the six trigger fire years. Most gauges contribute in all years; hybas_1120840690 (second column) is absent in 2018 and 2019, and hybas_1120842990 and hybas_1120843610 are absent in 2019.](../workflow/figures/adamawa/06_gauge_contributions_rp4_n6.png)

</details>
<!-- markdownlint-enable MD033 -->

---

## Readiness trigger

The readiness trigger uses both the GloFAS ensemble reforecast and the GloFAS reanalysis at Wuroboki, evaluated over 2003–2022 (n = 20 years). A year fires if either the reforecast ensemble mean exceeds the discharge threshold at any lead time within the maximum lead time window (Aug–Nov), or the reanalysis discharge exceeds the threshold on any wet-season day (LT = 0). The discharge threshold is fixed at the 2025 value of **3,132 m³/s** (~5-year GloFAS reanalysis RP), which sits above the 4-year reanalysis RP (3,009 m³/s) to reduce false alarms. Two lead time configurations were evaluated for the reforecast component:

- **Option A — 3,132 m³/s, LT ≤ 13 days:** fires in 2003, 2008, 2012, 2014, 2016, 2019, 2022 (k = 7, RP = 3.0 yr). POD = 3/4 action years (misses 2018). Lead times vs action trigger: 2012 +2d, 2019 +46d, 2022 +13d.
- **Option B — 3,250 m³/s, LT ≤ 12 days:** fires in 2003, 2014, 2016, 2019, 2022 (k = 6, RP = 3.5 yr). POD = 2/4 (misses 2012 and 2018). Lead times vs action trigger: 2012 −15d (fires after), 2019 +46d, 2022 +3d. The higher threshold means the GloFAS signal in 2012 (max 3,142 m³/s) never crosses the bar.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Figure: Readiness trigger POD vs lead time at Wuroboki (4-yr RP threshold)</summary>

![GloFAS readiness trigger performance vs lead time at Wuroboki (4-yr RP threshold). POD rises steeply between LT 8–13d and plateaus; FAR increases continuously. The selected cut-off at LT ≤ 13d sits just before FAR begins to climb sharply for the 4-yr RP benchmark.](../workflow/figures/adamawa/07_readiness_trigger_rp4.png)

</details>
<!-- markdownlint-enable MD033 -->

**Option A (3,132 m³/s, LT ≤ 13d) was selected.** The +2d lead in 2012 is operationally marginal (GloFAS peaks only 3,142 m³/s at LT=1d, meaning any higher threshold misses 2012 entirely), but Option A is preferable to Option B because it avoids a retroactive trigger in 2012. Both options fail to detect 2018; extending to LT ≤ 16d detects 2018 but adds a new false positive in 2010 and shortens the combined RP to 2.3 years — an unacceptable activation frequency. The GloFAS reforecast appears to have systematically underforecast the 2018 event at short lead times, which cannot be resolved by threshold or lead time adjustment alone.

The readiness trigger fires approximately once every 3 years, compared to the action trigger's once every 4.5 years. Roughly 4 readiness activations per 20 years will not be followed by an action trigger activation. This frequency is financially acceptable at 5% pre-positioning cost but should be communicated clearly to partners.

---

## Performance evaluation

### 2025 vs 2026 comparison

The two triggers differ in exactly one year each. The 2025 trigger fires in 2003 (a 3-year Floodscan event, not a 4-year event) but not in 2018. The 2026 trigger fires in 2018 (a genuine 4-year Floodscan event) but not in 2003. This directly reflects the design target shift from 5-year RP (2025) to 4-year RP (2026).

The improvement is concentrated at the 4-year RP benchmark (POD: 50% → 67%, F1: 0.50 → 0.67); performance at 3-year and 5-year levels is identical across both designs. Both miss 2015 and 2023, which are genuine 4–5-year flood years not captured by either design.

The 2026 design trades the 2025 trigger's two-model redundancy (GloFAS + GRRR) for spatial redundancy within a single model (10 geographically distributed GRRR gauges). This makes it more robust to local gauge noise but removes GloFAS as an independent cross-check. The GRRR reforecast horizon (≤7 days) is also shorter than GloFAS's, constraining maximum early warning from the action trigger alone — which is why a separate readiness trigger was developed.

### Performance summary

The 2026 action trigger was explicitly designed to preserve the **same activation frequency as 2025** — both fire 6 times in 26 reanalysis years (RP = 4.5 yr, 22% annual probability). The revision changes *which* years fire, improving alignment with the 4-year RP design target without altering the intervention frequency expected by the AA framework.

| | 2025 (endorsed) | 2026 (revised) |
|---|---|---|
| Action trigger RP | 4.5 years | 4.5 years |
| Annual activation probability | 22% | 22% |
| Fire years (GRRR reanalysis, 1998–2023) | 1998, 1999, 2003, 2012, 2019, 2022 | 1998, 1999, 2012, 2018, 2019, 2022 |
| Detection rate (POD) at 4-yr RP | 50%† | **67%** |
| FAR at 4-yr RP | 15%† | **10%** |
| F1 at 4-yr RP | 50%† | **67%** |
| Readiness trigger RP | — | ~3 years |
| Readiness fire years (GloFAS reforecast, 2003–2022) | — | 2003, 2008, 2012, 2014, 2016, 2019, 2022 |

Accuracy evaluated against Floodscan SFED annual maxima, 1998–2023 (n = 26 years) at the **4-year RP** design target. FAR = FP / (FP + TN). At 3-yr and 5-yr RP benchmarks, both triggers perform identically.

† 2025 4-yr metrics computed here on the same 1998–2023 period for direct comparability; 2025 figures at 3-yr and 5-yr RP are from the endorsed framework PDF and may reflect a different evaluation period.

**Small sample caveat:** With only 6 positive events at the 4-yr RP level, each individual year shifts POD and F1 by ~17 percentage points. All metrics should be treated as indicative rather than statistically robust.

### Readiness trigger performance

Readiness trigger configuration: **GloFAS > 3,132 m³/s at Wuroboki (G5004), either in the reforecast ensemble mean at lead time ≤ 13 days, or in the GloFAS reanalysis (LT = 0) on any wet-season day**. The reanalysis component acts as a same-day "observed conditions" signal alongside the short-range forecast. Evaluated over 2003–2022 (n = 20 complete reforecast years), benchmarked against the 2026 action trigger fire years within that window: {2012, 2018, 2019, 2022}.

| Metric | Value |
|---|---|
| Evaluation period | 2003–2022 (n = 20 years) |
| Benchmark | 2026 action trigger years in window: {2012, 2018, 2019, 2022} |
| TP — readiness fires, action fires | 3 (2012, 2019, 2022) |
| FP — readiness fires, no action | 4 (2003, 2008, 2014, 2016) |
| FN — readiness misses, action fires | 1 (2018) |
| TN — neither fires | 12 |
| Detection rate (POD) | 75% |
| False alarm rate (FAR = FP / FP+TN) | 25% |
| Precision | 43% |
| F1 | 55% |
| Activation return period | ~3 years |

Lead times ahead of the estimated action trigger were −3d (2012), +41d (2019), and +10d (2022). The 2022 figure benefits from the reanalysis component firing 2 days before the reforecast (19 Aug vs 21 Aug). The 2019 figure reflects an unusually early GloFAS signal; excluding that outlier, the observed range is −3 to +10 days. Full lead time detail relative to flood onset is in the Trigger timing performance section below. The 2018 miss cannot be resolved by threshold or lead time adjustment — the GloFAS ensemble and reanalysis both fall short of the 3,132 m³/s threshold in 2018.

**Small sample caveat:** The readiness trigger is benchmarked against only 4 action trigger years in the 2003–2022 evaluation window. Each year shifts POD by 25 percentage points; the 75% POD figure rests on 3 detections out of 4 opportunities and should not be read as a stable estimate of operational reliability.

### Trigger timing performance

Timing is measured relative to the **first day Floodscan SFED exceeds the 4-year RP threshold** ("RP exceedance"). Positive lead = trigger fired before the reference date. Action trigger dates are from the GRRR reanalysis (with a +5d offset applied as an estimate of reforecast lead); readiness trigger dates are from the GloFAS reforecast directly.

**Small sample caveat:** Lead times to RP exceedance are observed in at most 4 event years for the action trigger and 2 years for the readiness trigger. Single outlier years (e.g. 1999's +41d reanalysis lead) substantially affect any summary statistic. Individual year values are more meaningful than averages.

#### Action trigger timing

Lead times are from the **GRRR reanalysis** (historical proxy). The estimated reforecast fire date is the reanalysis date minus 5 days — i.e. the reforecast is expected to detect the signal 5 days earlier. These are illustrative estimates; in practice, reforecast lead will vary by year.

| Year | Reanalysis fires | Est. reforecast fires (−5d) | FS 4yr RP first crosses | Lead to RP (reanalysis) | Lead to RP (est. reforecast) |
|---|---|---|---|---|---|
| 1999 | 7 Sep | 2 Sep | 18 Oct | +41d | **+46d** |
| 2012 | 23 Aug | 18 Aug | 23 Aug | 0d | **+5d** |
| 2015 | — | — | 1 Sep | *miss* | — |
| 2018 | 13 Sep | 8 Sep | 12 Sep | −1d | **+4d** |
| 2022 | 3 Sep | 29 Aug | 28 Aug | −6d | **−1d** |
| 2023 | — | — | 6 Oct | *miss* | — |

Excluding the atypical 1999 season (an early upstream signal 41 days before RP exceedance), the GRRR reanalysis fires at roughly the same time the flood threshold is first crossed (0 to −6 days). With the −5d reforecast offset, the estimated operational lead to the 4yr RP crossing is **+4 to +5 days** (2012, 2018) or approximately −1 day in 2022 (flood rose unusually fast). The 1999 figure (+41d reanalysis, estimated **+46d** with forecast) is an outlier and not representative of the typical operational window.

#### Readiness trigger (GloFAS reforecast + reanalysis)

Lead times are from the **first readiness fire date**, which is the earlier of: the GloFAS reforecast (first issue date with ensemble mean > 3,132 m³/s at LT ≤ 13d), or the GloFAS reanalysis (first day reanalysis discharge > 3,132 m³/s). Evaluated against Floodscan 4-year RP event years only.

Lead to action trigger is relative to the **estimated reforecast fire date** (reanalysis − 5d): 18 Aug in 2012, 29 Aug in 2022.

| Year | Readiness fires | Source | Lead to RP exceedance | Lead to est. action trigger | Notes |
|---|---|---|---|---|---|
| 2012 | 21 Aug | Reforecast | +2d | −3d | TP — readiness fires 3d *after* est. action |
| 2018 | — | — | — | — | FN — GloFAS underforecast and reanalysis below threshold |
| 2022 | 19 Aug | Reanalysis | +9d | +10d | TP — readiness fires 10d before est. action |

In 2022 the reanalysis fires 2 days before the reforecast (19 Aug vs 21 Aug), giving **9 days** ahead of the RP exceedance and **10 days** ahead of the estimated action trigger. In 2012 the reanalysis does not reach the threshold, so the fire date remains 21 Aug from the reforecast alone, still 3 days after the estimated action trigger date. The 2018 miss is irreducible — neither the reforecast at LT ≤ 13d nor the reanalysis exceeds 3,132 m³/s in 2018. No forecast offset is needed for the reforecast component; the reanalysis is an ERA5-driven product available with a short lag in near-real-time.

#### Lead to flood peak — all triggered years

Lead from first GloFAS issue date to the Floodscan wet-season annual maximum. Includes false positive years where the readiness fires but flood intensity did not reach the 4-yr RP level. 4-yr RP event years that the readiness missed (2015, 2018) are shown for reference.

| Year | Readiness fires | Source | FS peak date | Lead to FS peak | FS 4-yr event |
|---|---|---|---|---|---|
| 2003 | 28 Aug | Reforecast | 21 Sep | +24d | FP |
| 2008 | 07 Sep | Reforecast | 17 Sep | +10d | FP |
| 2012 | 21 Aug | Reforecast | 21 Sep | +31d | TP |
| 2014 | 28 Aug | Reforecast | 31 Aug | +3d | FP |
| 2015 | — | — | 05 Sep | — | FN |
| 2016 | 04 Sep | Reforecast | 25 Sep | +21d | FP |
| 2018 | — | — | 12 Sep | — | FN |
| 2019 | 31 Aug | Reforecast | 30 Oct | +60d | FP |
| 2022 | 19 Aug | Reanalysis | 21 Sep | +33d | TP |

In the two TP years the readiness trigger fires roughly a month before the Floodscan peak: 31 days in 2012, and 33 days in 2022 (with the reanalysis firing 2 days earlier than the reforecast would have). Even in most FP years there is meaningful lead time — the only exception is 2014 where the readiness fires just 3 days before a minor wet-season peak that did not reach the 4-yr level.

---

## Trigger specifications

### Trigger definitions

**2025 action trigger (endorsed Aug 2025):**

- GloFAS: ensemble mean discharge ≥ 3,132 m³/s at Wuroboki (G5004) within next 5 days
- OR Google GRRR: discharge ≥ 1,195 m³/s at Kangli (hybas\_1120842550) at any point in wet season

**2026 action trigger (revised):**

- Google GRRR: ≥ 6 of 10 gauges simultaneously exceed their individual 4-year empirical RP threshold on the same wet-season day
- Individual threshold: approximately 1,100–1,117 m³/s at each of 9 main-channel gauges; 143 m³/s at outlier gauge hybas\_1120840690
- The single-station Google condition at Kangli is replaced by a spatially distributed multi-gauge voting rule. GloFAS is moved to a separate, independent readiness trigger.

**2026 readiness trigger (new — not in 2025 framework):**

- GloFAS reforecast ensemble mean > 3,132 m³/s at Wuroboki within lead time ≤ 13 days, **or**
- GloFAS reanalysis > 3,132 m³/s at Wuroboki (LT = 0, near-real-time)
- Designed to fire ahead of the action trigger and enable pre-positioning

### Action trigger gauge thresholds

The action trigger fires if **≥ 6 of the 10 gauges below** simultaneously exceed their individual 4-year empirical RP threshold on the same wet-season day. Thresholds were estimated using Weibull plotting positions on wet-season annual maxima, 1998–2023. Gauges are ordered by Spearman ρ (descending); all are GRRR source.

| Gauge ID | Latitude | Longitude | Spearman ρ | 4-yr RP threshold (m³/s) | Trigger years contributed (of 6) |
|---|---|---|---|---|---|
| hybas\_1120842990 | 9.39°N | 12.81°E | 0.742 | 1,111 | 5 |
| hybas\_1120843610 | 9.37°N | 12.85°E | 0.742 | 1,102 | 5 |
| hybas\_1120845060 | 9.33°N | 12.71°E | 0.732 | 1,102 | 6 |
| hybas\_1120849600 | 9.24°N | 12.58°E | 0.728 | 1,114 | 6 |
| hybas\_1120848550 | 9.26°N | 12.49°E | 0.726 | 1,106 | 6 |
| hybas\_1121970280 | 9.31°N | 12.44°E | 0.723 | 1,110 | 6 |
| hybas\_1120842550 *(Kangli)* | 9.39°N | 12.37°E | 0.719 | 1,114 | 6 |
| hybas\_1120840700 | 9.44°N | 12.36°E | 0.719 | 1,113 | 6 |
| hybas\_1120840560 | 9.45°N | 12.35°E | 0.719 | 1,117 | 6 |
| hybas\_1120840690 *(outlier — small tributary)* | 9.44°N | 12.34°E | 0.711 | 143 | 4 |

hybas\_1120840690 has a substantially lower threshold (143 m³/s vs ~1,100 m³/s for the main-channel gauges) and contributed to only 4 of 6 trigger years. It is retained in the pool because a single anomalous gauge cannot dominate the ≥6/10 voting rule.

### Historical event and trigger year overview

The heatmap below shows Floodscan flood event years and trigger activation years across the full evaluation period (1998–2023 for the action trigger; 2003–2022 for the readiness trigger). Numbers in trigger rows indicate lead time in days relative to the first Floodscan ≥4-yr RP exceedance that season (positive = trigger fired before flood; shown only in years where Floodscan reached ≥4-yr RP).

<!-- markdownlint-disable MD033 -->
<div class="heatmap-wrap">
<table class="heatmap">
<thead><tr>
<th class="heatmap-label"></th>
<th class="heatmap-year">1998</th>
<th class="heatmap-year">1999</th>
<th class="heatmap-year">2000</th>
<th class="heatmap-year">2001</th>
<th class="heatmap-year">2002</th>
<th class="heatmap-year">2003</th>
<th class="heatmap-year">2004</th>
<th class="heatmap-year">2005</th>
<th class="heatmap-year">2006</th>
<th class="heatmap-year">2007</th>
<th class="heatmap-year">2008</th>
<th class="heatmap-year">2009</th>
<th class="heatmap-year">2010</th>
<th class="heatmap-year">2011</th>
<th class="heatmap-year">2012</th>
<th class="heatmap-year">2013</th>
<th class="heatmap-year">2014</th>
<th class="heatmap-year">2015</th>
<th class="heatmap-year">2016</th>
<th class="heatmap-year">2017</th>
<th class="heatmap-year">2018</th>
<th class="heatmap-year">2019</th>
<th class="heatmap-year">2020</th>
<th class="heatmap-year">2021</th>
<th class="heatmap-year">2022</th>
<th class="heatmap-year">2023</th>
</tr></thead>
<tbody>
<tr>
<td class="heatmap-label">Floodscan ≥3-yr RP</td>
<td class="heatmap-cell" style="background:#4A90E2" title="1998"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="1999"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2000"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2001"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2002"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2003"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2004"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2005"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2006"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2007"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2008"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2009"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2010"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2011"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2012"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2013"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2014"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2015"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2016"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2017"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2018"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2019"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2020"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2021"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2022"></td>
<td class="heatmap-cell" style="background:#4A90E2" title="2023"></td>
</tr>
<tr>
<td class="heatmap-label">Floodscan ≥4-yr RP</td>
<td class="heatmap-cell" style="background:#eef1f4" title="1998"></td>
<td class="heatmap-cell" style="background:#007CE0" title="1999"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2000"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2001"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2002"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2003"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2004"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2005"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2006"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2007"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2008"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2009"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2010"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2011"></td>
<td class="heatmap-cell" style="background:#007CE0" title="2012"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2013"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2014"></td>
<td class="heatmap-cell" style="background:#007CE0" title="2015"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2016"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2017"></td>
<td class="heatmap-cell" style="background:#007CE0" title="2018"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2019"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2020"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2021"></td>
<td class="heatmap-cell" style="background:#007CE0" title="2022"></td>
<td class="heatmap-cell" style="background:#007CE0" title="2023"></td>
</tr>
<tr>
<td class="heatmap-label">Floodscan ≥5-yr RP</td>
<td class="heatmap-cell" style="background:#eef1f4" title="1998"></td>
<td class="heatmap-cell" style="background:#1E5A8E" title="1999"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2000"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2001"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2002"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2003"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2004"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2005"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2006"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2007"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2008"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2009"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2010"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2011"></td>
<td class="heatmap-cell" style="background:#1E5A8E" title="2012"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2013"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2014"></td>
<td class="heatmap-cell" style="background:#1E5A8E" title="2015"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2016"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2017"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2018"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2019"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2020"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2021"></td>
<td class="heatmap-cell" style="background:#1E5A8E" title="2022"></td>
<td class="heatmap-cell" style="background:#1E5A8E" title="2023"></td>
</tr>
<tr class="heatmap-spacer"><td colspan="27"></td></tr>
<tr>
<td class="heatmap-label">Action trigger</td>
<td class="heatmap-cell" style="background:#F2645A" title="1998"></td>
<td class="heatmap-cell" style="background:#F2645A" title="1999"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2000"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2001"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2002"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2003"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2004"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2005"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2006"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2007"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2008"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2009"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2010"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2011"></td>
<td class="heatmap-cell" style="background:#F2645A" title="2012"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2013"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2014"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2015"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2016"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2017"></td>
<td class="heatmap-cell" style="background:#F2645A" title="2018"></td>
<td class="heatmap-cell" style="background:#F2645A" title="2019"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2020"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2021"></td>
<td class="heatmap-cell" style="background:#F2645A" title="2022"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2023"></td>
</tr>
<tr>
<td class="heatmap-label">Readiness trigger</td>
<td class="heatmap-cell" style="background:#d8dde3" title="1998"></td>
<td class="heatmap-cell" style="background:#d8dde3" title="1999"></td>
<td class="heatmap-cell" style="background:#d8dde3" title="2000"></td>
<td class="heatmap-cell" style="background:#d8dde3" title="2001"></td>
<td class="heatmap-cell" style="background:#d8dde3" title="2002"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2003"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2004"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2005"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2006"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2007"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2008"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2009"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2010"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2011"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2012"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2013"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2014"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2015"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2016"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2017"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2018"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2019"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2020"></td>
<td class="heatmap-cell" style="background:#eef1f4" title="2021"></td>
<td class="heatmap-cell" style="background:#1EBFB3" title="2022"></td>
<td class="heatmap-cell" style="background:#d8dde3" title="2023"></td>
</tr>
</tbody>
</table>
<div class="heatmap-legend">
<span class="heatmap-legend-item"><span class="heatmap-swatch" style="background:#4A90E2;"></span>Floodscan ≥3-yr RP</span>
<span class="heatmap-legend-item"><span class="heatmap-swatch" style="background:#007CE0;"></span>Floodscan ≥4-yr RP</span>
<span class="heatmap-legend-item"><span class="heatmap-swatch" style="background:#1E5A8E;"></span>Floodscan ≥5-yr RP</span>
<span class="heatmap-legend-item"><span class="heatmap-swatch" style="background:#F2645A;"></span>Action trigger fired</span>
<span class="heatmap-legend-item"><span class="heatmap-swatch" style="background:#1EBFB3;"></span>Readiness trigger fired</span>
<span class="heatmap-legend-item"><span class="heatmap-swatch" style="background:#d8dde3; border:1px solid #bbc4ce;"></span>Outside eval. period</span>
</div>
<p class="heatmap-caption">Coloured cells indicate trigger activation (action trigger: coral; readiness trigger: teal). Floodscan flood event years are shown at three RP levels. Grey cells in the readiness row indicate years outside the GloFAS reforecast evaluation period (2003–2022). Lead time detail is in the Trigger timing performance section above.</p>
</div>
<!-- markdownlint-enable MD033 -->

---

## NHF flash flood trigger

The NHF trigger targets **flash flooding** in BAY states (Borno, Adamawa, Yobe) — a distinct flood type and funding mechanism from the CERF riverine trigger above. Flash flood risk is based on the **NiHSA 2026 (Nigeria Humanitarian Snapshot Assessment)** at-risk communities dataset, which classifies 7,210 communities across BAY states into high, medium, and low inundation depth zones (high: 1,190; medium: 5,545; low: 475).

### LGA targeting methodology

LGAs are ranked by a weighted composite count of NiHSA at-risk communities, where the composite score = (High-risk × 3) + (Medium-risk × 2) + (Low-risk × 1). Weights reflect relative severity: high-depth zones carry three times the weight of low-depth zones. HNRP (Humanitarian Needs Revised Priority) status from OCHA's 2026 prioritisation is shown as a contextual indicator but does not influence the quantitative ranking.

### Candidate LGAs

Seven LGAs are identified as candidates for 2026 NHF targeting; **4 are expected to be selected following an ongoing LGA review**. Ranked by composite score:

| Rank | LGA | State | High-risk | Medium-risk | Low-risk | Composite score | HNRP priority |
|---|---|---|---|---|---|---|---|
| 1 | Mobbar | Borno | 377 | 309 | 0 | 1,395 | Yes |
| 2 | Geidam | Yobe | 207 | 133 | 0 | 747 | — |
| 3 | Gubio | Borno | 105 | 221 | 77 | 712 | Yes |
| 4 | Ngala | Borno | 76 | 188 | 0 | 452 | Yes |
| 5 | Maiduguri | Borno | 73 | 196 | 0 | 442 | Yes |
| 6 | Numan | Adamawa | 125 | 31 | 0 | 406 | — |
| 7 | Yola North | Adamawa | 102 | 128 | 0 | 358 | — |

4 of the 7 candidates (Mobbar, Gubio, Ngala, Maiduguri) are HNRP-prioritised, indicating good alignment between the quantitative flash flood ranking and OCHA's humanitarian prioritisation framework. Of these, **Maiduguri and Ngala** also appeared in the 2025 NHF trigger, providing continuity; the other four 2025 trigger LGAs (Bade, Bama, Karasuwa, Madagali) do not appear in the 2026 top-7 under the revised scoring.

---

## Appendix

### What was not resolved

**GloFAS reanalysis as a supplementary readiness component** was evaluated and not recommended. Adding a condition that fires if the GloFAS ERA5 reanalysis exceeds 3,132 m³/s (as a near-real-time nowcast proxy) produces no additional event detections within the 2003–2022 evaluation window — the reanalysis only adds 2022 and 2003, both already captured by the reforecast component — and provides only a 2-day earlier signal in 2022. Its main theoretical value is as a safety net for years where the reforecast fails, but realising this benefit would require a reliable near-real-time GloFAS data feed that is not currently operational in this workflow.

**2015 and 2023 remain unexplained misses.** Both are genuine 4–5-year Floodscan events that neither trigger configuration detects. Whether this reflects a different flood pathway (e.g. Lagdo dam release timing, upper Benue vs tributary contributions), gaps in GRRR gauge coverage for those years, or a model deficiency is not yet determined. Investigating these years is a priority before operational deployment.

**Benue state** was initially in scope as a second state for trigger development. Following the Adamawa analysis, the decision was made to keep focus on Adamawa and not proceed with a Benue trigger for the 2026 framework. The workflow is transferable: notebooks `05_trigger_assessment.ipynb` and `06_trigger_definition.ipynb` are ready to run with `STATE = "Benue"`, and the same methodology applies directly once the optimal RP threshold and gauge count for Makurdi are calibrated in `STATE_CONFIG`.

---

### Year-by-year peak timing

Per-event detail plots showing Floodscan SFED (amber), GloFAS reanalysis (blue), individual GRRR gauge series normalised to each gauge's 4-year RP threshold (coral), and trigger activation markers. Vertical lines mark: ≥60% GRRR gauges threshold (coral dashed), Floodscan RP exceedance (amber dashed), and GloFAS readiness trigger issue date (teal). A news event timeline is shown below each plot.

<!-- markdownlint-disable MD033 -->

#### Floodscan ≥4-yr RP event years

<details>
<summary>1999 — Floodscan ≥4-yr RP event · Action trigger TP (+41d to RP) · Readiness: outside eval. period</summary>

![1999 peak timing detail — GRRR fires 7 Sep, Floodscan RP exceedance 18 Oct, annual peak 21 Oct](../workflow/figures/adamawa/exploration_peak_timing_1999_detail.png){width=100%}

</details>

<details>
<summary>2012 — Floodscan ≥4-yr RP event · Action trigger TP (0d to RP) · Readiness TP (+2d to RP)</summary>

![2012 peak timing detail — GRRR and Floodscan both cross threshold 23 Aug, readiness fires 21 Aug, annual peak 21 Sep](../workflow/figures/adamawa/exploration_peak_timing_2012_detail.png){width=100%}

</details>

<details>
<summary>2015 — Floodscan ≥4-yr RP event · Both triggers missed</summary>

![2015 peak timing detail — no GRRR signal, Floodscan RP exceedance 1 Sep, annual peak 5 Sep](../workflow/figures/adamawa/exploration_peak_timing_2015_detail.png){width=100%}

</details>

<details>
<summary>2018 — Floodscan ≥4-yr RP event · Action trigger TP (−1d to RP) · Readiness missed</summary>

![2018 peak timing detail — GRRR fires 13 Sep, Floodscan RP exceedance and annual peak both 12 Sep, GloFAS reforecast did not exceed threshold at LT≤13d](../workflow/figures/adamawa/exploration_peak_timing_2018_detail.png){width=100%}

</details>

<details>
<summary>2022 — Floodscan ≥4-yr RP event · Action trigger TP (−6d to RP, +18d to peak) · Readiness TP (+7d to RP)</summary>

![2022 peak timing detail — Floodscan RP exceedance 28 Aug, readiness fires 21 Aug, GRRR fires 3 Sep, annual peak 21 Sep](../workflow/figures/adamawa/exploration_peak_timing_2022_detail.png){width=100%}

</details>

<details>
<summary>2023 — Floodscan ≥4-yr RP event · Both triggers missed</summary>

![2023 peak timing detail — no GRRR signal, Floodscan RP exceedance 6 Oct, annual peak 9 Oct](../workflow/figures/adamawa/exploration_peak_timing_2023_detail.png){width=100%}

</details>

#### Action trigger false positive years

<details>
<summary>1998 — GRRR fires (false positive against 4-yr RP) · No Floodscan ≥4-yr RP event</summary>

![1998 peak timing — GRRR action trigger fires but Floodscan does not reach 4-yr RP threshold](../workflow/figures/adamawa/exploration_peak_timing_1998_fp.png){width=100%}

</details>

<details>
<summary>2019 — GRRR fires (false positive against 4-yr RP) · No Floodscan ≥4-yr RP event · Readiness also fires</summary>

![2019 peak timing — both action and readiness triggers fire but Floodscan does not reach 4-yr RP threshold](../workflow/figures/adamawa/exploration_peak_timing_2019_fp.png){width=100%}

</details>

#### Readiness trigger only years

<details>
<summary>2003 — Readiness trigger fires (GloFAS false positive)</summary>

![2003 readiness context — GloFAS reforecast fires but no GRRR signal and no 4-yr RP Floodscan event](../workflow/figures/adamawa/exploration_peak_timing_2003_readiness.png){width=100%}

</details>

<details>
<summary>2008 — Readiness trigger fires (GloFAS false positive)</summary>

![2008 readiness context — GloFAS reforecast fires but no GRRR signal and no 4-yr RP Floodscan event](../workflow/figures/adamawa/exploration_peak_timing_2008_readiness.png){width=100%}

</details>

<details>
<summary>2014 — Readiness trigger fires (GloFAS false positive)</summary>

![2014 readiness context — GloFAS reforecast fires but no GRRR signal and no 4-yr RP Floodscan event](../workflow/figures/adamawa/exploration_peak_timing_2014_readiness.png){width=100%}

</details>

<details>
<summary>2016 — Readiness trigger fires (GloFAS false positive)</summary>

![2016 readiness context — GloFAS reforecast fires but no GRRR signal and no 4-yr RP Floodscan event](../workflow/figures/adamawa/exploration_peak_timing_2016_readiness.png){width=100%}

</details>

<!-- markdownlint-enable MD033 -->
