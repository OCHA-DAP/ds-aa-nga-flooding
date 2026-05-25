# Context & Background

[← Index](index.html)

---

## 2025 analysis

### Approach

The 2025 analysis combined multiple forecasting and observational data sources to develop a flood trigger for the **Wuroboki station** (Adamawa state):

| Source | Role |
|---|---|
| GloFAS reanalysis (1998–2024) | Primary discharge signal (`dis24`) |
| Google GRRR reanalysis & reforecast | Complementary streamflow signal |
| Floodscan SFED | Observational ground truth (satellite flood extent) |
| NiHSA water levels | Observational records at Wuroboki |

### Trigger definition

A **dual-source OR trigger** was found to give the best balance of detection accuracy and lead time:

| Source | Threshold | Approx. RP |
|---|---|---|
| GloFAS | 3,132 m³/s | ~5.4 yr |
| Google GRRR | 1,195 m³/s | ~5.4 yr |

**Logic:** trigger activates when *either* GloFAS or Google GRRR exceeds its threshold. The combined activation frequency gives an overall ~4.5-year RP, targeting 5-year RP flooding events (as required by CERF).

Floodscan wet-season (Jul–Nov) annual maxima were used as observational ground truth for return period calibration.

### Key findings

- GloFAS peaks typically **10–20+ days** ahead of observed flooding; Google GRRR provides **15–25 days** lead time
- The OR-logic trigger maximised F1 score while maintaining 20+ days average lead time
- Mean pixel aggregation (rather than admin-level aggregation) was selected for Floodscan
- Analysis was based on reanalysis throughout; operational reforecasts showed lower but acceptable performance

---

## Stations

| Station | River | State | GloFAS key | Google HYBAS |
|---|---|---|---|---|
| Wuroboki | Benue | Adamawa | `wuroboki` | `hybas_1120842550` |
| Makurdi | Benue | Benue | `makurdi` | `hybas_1120911340` |

---

## Data sources

| Dataset | Path / location | Notes |
|---|---|---|
| GloFAS reanalysis | Azure blob | Via `src.datasources.glofas` |
| Google GRRR reanalysis | `gs://flood-forecasting/…/reanalysis/streamflow.zarr/` | Via `src.datasources.grrr` |
| Google GRRR reforecast | `gs://flood-forecasting/…/reforecast/streamflow.zarr/` | Via `src.datasources.grrr` |
| Google HYBAS locations | `gs://flood-forecasting/…/hybas_outlet_locations_UNOFFICIAL.zarr/` | Gauge lat/lon only |
| Floodscan SFED | Azure blob — `floodscan/daily/v5/processed/` | 300s COGs, 1998–2025 |
| Processed Floodscan (Benue) | `ds-aa-nga-flooding/processed/floodscan/fs_benue_state_pixels_1998_2025.parquet` | Pixel-level, ~10km river buffer |
| Processed Floodscan (Adamawa) | `ds-aa-nga-flooding/processed/floodscan/fs_adamawa_pixels_1998_2025.parquet` | Pixel-level, ~10km river buffer |
| NiHSA risk communities | `ds-aa-nga-flooding/raw/AA-nigeria_data/NiHSA/AFO_communities_atrisk_2026.csv` | `lat`, `lon`, `depth_zone` |
| Google inundation history | `ds-aa-nga-flooding/processed/google_inundation_history/combined_nga.parquet` | Geoparquet |
| WorldPop 2020 (1km) | WorldPop STAC — `nga_pop_2020_…_1km_…` | Via `worldpop.load_worldpop_from_stac()` |
