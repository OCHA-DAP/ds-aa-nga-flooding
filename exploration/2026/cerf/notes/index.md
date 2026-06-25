# CERF Nigeria Flooding — Working Notes

Analysis of riverine flooding in Nigeria for CERF anticipatory action, focused on Adamawa and Benue states along the Benue river.

## Reference

- [Context & background](context.html) — 2025 analysis summary, trigger definition, data sources
- [Methodology](methodology.html) — workflow steps, configuration levers, and how to extend to other river systems
- [2026 trigger development](trigger_development.html) — narrative of design decisions: gauge selection, action trigger calibration, readiness trigger options, open questions

---

## Sessions

| Date | Summary |
|---|---|
| [2026-05-22](2026-05-22.html) | Centralised `STATE_CONFIG` in `constants.py`; added Google HYBAS gauge IDs; updated notebooks 12–14 |
| [2026-05-25](2026-05-25.html) | Data source and constants overhaul; new `13_model_performance.ipynb`; initial `14_forecast_performance.ipynb` |
| [2026-05-26](2026-05-26.html) | Metric and plot refinements to `14_forecast_performance.ipynb`; new `15_trigger_performance.ipynb` evaluating GloFAS reforecast vs Floodscan events |
| [2026-05-27](2026-05-27.html) | Consistency fixes (NB 08, 14, 15); 2025 threshold methodology investigation; new `17_threshold_selection.ipynb` (Benue grid search) and `18_trigger_summary_performance.ipynb` |
| [2026-05-28](2026-05-28.html) | New `19_model_reanalysis_comparison.ipynb` (GRRR gauge correlation + annual peak diagnostics) and `20_trigger_tuning.ipynb` (multi-gauge trigger grid search); dashboard collapsible sections; Floodscan threshold bug fix |
| [2026-06-18](2026-06-18.html) | 2025 vs 2026 trigger comparison (metrics, pros/cons); GloFAS readiness trigger configuration options and lead time analysis; operational limitations for stakeholders |
