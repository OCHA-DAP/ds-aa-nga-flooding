"""Data-driven config for the multi-state Niger/Benue flood trigger.

The registry (LGAs + forecast gauges/stations) is the single source of truth,
persisted to blob and read by the analysis workflow, the web-data generator, and
the monitoring app. `presets` defines named subsets over the registry and the
`resolve_selection` resolver (preset + include/exclude overrides) that every
consumer calls to get a concrete set of LGAs and gauges.

Build/refresh the registry with `pipelines/build_config_registry.py`.
"""

from src.config.presets import (  # noqa: F401
    BASINS,
    list_presets,
    resolve_selection,
)
from src.config.registry import (  # noqa: F401
    GAUGE_REGISTRY_BLOB,
    LGA_REGISTRY_BLOB,
    STATE_PARAMS_BLOB,
    load_gauge_registry,
    load_lga_registry,
    load_state_params,
)
