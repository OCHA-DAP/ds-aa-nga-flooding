"""Named subsets over the config registry, and the selection resolver.

A *preset* is a named or namespaced filter over the LGA registry:

    "all_riverine"        every riverine LGA (14 states)
    "nihsa_hfr"           NiHSA High-Flood-Risk LGAs only
    "adamawa_endorsed"    the 7 endorsed 2025 framework LGAs
    "basin:Benue"         one HydroBASINS-L4 basin (Benue / Upper Niger /
                          Lower Niger / Niger Delta) — mirrors the live R pilot
    "state:Kogi"          one of the 14 states

`resolve_selection` turns a preset plus optional include/exclude overrides into a
concrete set of LGAs and the gauges that inform them. The monitoring app, the
web-data generator, and the analysis workflow all call this one function so the
selection is consistent everywhere.
"""

from dataclasses import dataclass, field

import pandas as pd

BASINS = ["Benue", "Upper Niger", "Lower Niger", "Niger Delta"]

# Named presets → boolean column on the LGA registry.
_NAMED_PRESETS = {
    "all_riverine": "is_riverine",
    "nihsa_hfr": "is_hfr",
    "adamawa_endorsed": "is_endorsed_adamawa",
}

# Default aggregation: a group (state or basin) fires when at least
# CONSENSUS_FRAC of its selected gauges exceed their RP threshold on a day.
# 0.6 matches the 2026 Adamawa design (≥6 of 10); the R pilot uses 0.8 per basin.
DEFAULT_CONSENSUS_FRAC = 0.6
DEFAULT_GROUP_BY = "state"


@dataclass
class Selection:
    """A resolved selection of LGAs and the gauges that inform them."""

    preset: str
    lgas: pd.DataFrame
    gauges: pd.DataFrame
    group_by: str = DEFAULT_GROUP_BY
    consensus_frac: float = DEFAULT_CONSENSUS_FRAC
    overrides: dict = field(default_factory=dict)

    def groups(self):
        """Aggregation groups → (n_gauges, n_required) under the consensus rule."""
        out = {}
        for g, sub in self.gauges.groupby(self.group_by):
            n = len(sub)
            out[g] = {"n_gauges": n, "n_required": self._required(n)}
        return out

    def _required(self, n):
        import math

        return max(1, math.ceil(self.consensus_frac * n)) if n else 0

    def summary(self):
        return {
            "preset": self.preset,
            "n_lgas": len(self.lgas),
            "n_states": self.lgas["state"].nunique(),
            "n_gauges": len(self.gauges),
            "group_by": self.group_by,
            "consensus_frac": self.consensus_frac,
            "groups": self.groups(),
        }


def list_presets(lga_registry):
    """Return all available preset identifiers for a given registry."""
    presets = list(_NAMED_PRESETS)
    presets += [f"basin:{b}" for b in BASINS if b in set(lga_registry["basin"])]
    presets += [f"state:{s}" for s in sorted(lga_registry["state"].unique())]
    return presets


def _preset_mask(preset, lga_registry):
    if preset in _NAMED_PRESETS:
        col = _NAMED_PRESETS[preset]
        if col not in lga_registry.columns:
            raise KeyError(f"registry missing column '{col}' for preset '{preset}'")
        return lga_registry[col].fillna(False).astype(bool)
    if ":" in preset:
        kind, val = preset.split(":", 1)
        if kind == "basin":
            return lga_registry["basin"] == val
        if kind == "state":
            return lga_registry["state"] == val
    raise ValueError(
        f"unknown preset '{preset}'. Use list_presets(lga_registry) to see options."
    )


def resolve_selection(
    preset,
    lga_registry,
    gauge_registry,
    *,
    include_lgas=(),
    exclude_lgas=(),
    include_gauges=(),
    exclude_gauges=(),
    gauge_sources=("grrr", "glofas"),
    group_by=DEFAULT_GROUP_BY,
    consensus_frac=DEFAULT_CONSENSUS_FRAC,
):
    """Resolve a preset + overrides into a concrete Selection.

    Parameters
    ----------
    preset : str
        A named or namespaced preset (see module docstring / list_presets).
    lga_registry, gauge_registry : DataFrame
        From src.config.registry.load_*.
    include_lgas / exclude_lgas : iterable of pcode
        Add / remove LGAs on top of the preset.
    include_gauges / exclude_gauges : iterable of gauge_id
        Add / remove gauges on top of the LGA-derived gauge set.
    gauge_sources : iterable
        Restrict gauges by source ("grrr", "glofas").
    group_by : {"state", "basin"}
        Aggregation grouping for the consensus rule.
    consensus_frac : float
        Fraction of a group's gauges that must exceed threshold to fire.
    """
    mask = _preset_mask(preset, lga_registry)
    pcodes = set(lga_registry.loc[mask, "pcode"])
    pcodes |= set(include_lgas)
    pcodes -= set(exclude_lgas)
    lgas = lga_registry[lga_registry["pcode"].isin(pcodes)].copy()

    g = gauge_registry
    sel_gauge = (
        g["lga_pcode"].isin(pcodes)
        & g["source"].isin(gauge_sources)
    )
    gauge_ids = set(g.loc[sel_gauge, "gauge_id"])
    gauge_ids |= set(include_gauges)
    gauge_ids -= set(exclude_gauges)
    gauges = g[g["gauge_id"].isin(gauge_ids)].copy()

    return Selection(
        preset=preset,
        lgas=lgas,
        gauges=gauges,
        group_by=group_by,
        consensus_frac=consensus_frac,
        overrides={
            "include_lgas": list(include_lgas),
            "exclude_lgas": list(exclude_lgas),
            "include_gauges": list(include_gauges),
            "exclude_gauges": list(exclude_gauges),
            "gauge_sources": list(gauge_sources),
        },
    )
