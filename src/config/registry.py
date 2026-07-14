"""Loaders for the config registry (LGAs + forecast gauges/stations).

The registry holds attributes only (no geometry) so it stays light and
DB/parquet-friendly; the monitoring app joins CODAB polygons by ``pcode`` and
plots gauges from their ``lat``/``lon`` columns.

Schema
------
lga_registry (one row per candidate LGA)
    pcode, name, state, basin, river_system,
    is_riverine, is_hfr, is_endorsed_adamawa,
    coverage_r            # Hannah's best Google-gauge correlation (HFR LGAs)
    floodscan_threshold   # flash-flood observational threshold (filled later)

gauge_registry (one row per forecast point)
    gauge_id, source(grrr|glofas), lat, lon,
    state, basin, lga_pcode, lga_name,   # nearest riverine LGA it informs
    quality_verified,
    best_r, best_lag,     # forecast skill vs Floodscan (filled by workflow)
    rp_threshold,         # per-gauge return-period threshold (filled by workflow)
    lead_time             # effective forecast lead time (days)

state_params (one row per state — the scalar "configuration levers")
    state, adm1_pcode, glofas_station, glofas_station_lat, glofas_station_lon,
    basins, n_lgas, n_gauges_grrr, n_gauges_glofas, median_coverage_r,
    analysis_start_year, analysis_end_year, rp_target, consensus_frac,
    glofas_readiness_thresh, glofas_readiness_leadtime   # filled by Phase 3
"""

import ocha_stratus as stratus

from src.constants import PROJECT_PREFIX

LGA_REGISTRY_BLOB = f"{PROJECT_PREFIX}/processed/config/lga_registry.parquet"
GAUGE_REGISTRY_BLOB = f"{PROJECT_PREFIX}/processed/config/gauge_registry.parquet"
STATE_PARAMS_BLOB = f"{PROJECT_PREFIX}/processed/config/state_params.parquet"


def load_lga_registry():
    """Load the LGA registry from blob."""
    return stratus.load_parquet_from_blob(LGA_REGISTRY_BLOB)


def load_gauge_registry():
    """Load the gauge/station registry from blob."""
    return stratus.load_parquet_from_blob(GAUGE_REGISTRY_BLOB)


def load_state_params():
    """Load the per-state parameters table from blob."""
    return stratus.load_parquet_from_blob(STATE_PARAMS_BLOB)
