---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-aa-nga-flooding
    language: python
    name: ds-aa-nga-flooding
---

# Combined Google-GloFAS trigger - reanalysis
<!-- markdownlint-disable MD013 -->
Determine best balance of Google and GloFAS forecasts to maximize accuracy and leadtime.

For simplicity, just doing now with reanalysis.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from tqdm.auto import tqdm

from src.datasources import glofas, grrr
from src.constants import *
from src.utils import rp_calc
```

## Load data

### GloFAS

```python
df_gf = glofas.load_glofas_reanalysis(station_name="wuroboki")
```

```python
df_gf = df_gf.rename(columns={"time": "valid_time"})
```

```python
df_gf
```

### Floodscan

```python
df_fs_raw = stratus.load_parquet_from_blob(
    f"{PROJECT_PREFIX}/processed/floodscan/fs_benue_pixels_1998_2024.parquet"
)
df_fs = (
    df_fs_raw.groupby("date")["SFED"]
    .mean()
    .reset_index()
    .rename(columns={"date": "valid_time"})
)
```

```python
df_fs
```

### Google

```python
ds_ra = grrr.load_reanalysis()
df_grrr = grrr.process_reanalysis(ds_ra)
```

```python
df_grrr
```

## Process

```python
df_compare = df_gf.merge(df_fs).merge(df_grrr)
```

```python
df_compare["year"] = df_compare["valid_time"].dt.year
```

```python
df_yearly_max = (
    df_compare.groupby("year").max().drop(columns=["valid_time"]).reset_index()
)
```

Determine dates of peak for each indicator.

```python
for col in ["dis24", "SFED", "streamflow"]:
    df_yearly_max[f"{col}_maxdate"] = df_compare.loc[
        df_compare.groupby("year").idxmax()[col]
    ]["valid_time"].values
```

Leadtime between peaks.

```python
for col in ["dis24", "streamflow"]:
    df_yearly_max[f"{col}_peak_leadtime"] = (
        df_yearly_max["SFED_maxdate"] - df_yearly_max[f"{col}_maxdate"]
    )
```

Return periods.

```python
for col in ["dis24", "SFED", "streamflow"]:
    df_yearly_max = rp_calc.calculate_one_group_rp(
        df_yearly_max, col_name=col, ascending=False
    )
```

Set whether to target each year, depending on return period severity.

This is admittedly not the best way to do this (would be easier filtering by ranks later), but will leave it for now because the rest of the code was already set up for this format.

```python
rp_targets = [3, 4, 5]
```

```python
for rp_target in rp_targets:
    df_yearly_max[f"{rp_target}yr_target"] = (
        df_yearly_max["SFED_rp"] >= rp_target
    )
```

```python
df_yearly_max
```

Quick comparing peak leadtimes. As expected GloFAS is quite early.

```python
df_yearly_max.set_index("year")[
    [f"{x}_peak_leadtime" for x in ["dis24", "streamflow"]]
].plot()
```

Check against severity of flooding. Slight positive relationship? Which is good, because this means that the more severe flooding is, the earlier we will be.

```python
df_yearly_max.set_index("SFED")[
    [f"{x}_peak_leadtime" for x in ["dis24", "streamflow"]]
].plot(marker=".", linewidth=0)
```

```python
df_in = pd.DataFrame(columns=["year"])

for rp_target in rp_targets:
    dicts = []
    thresh_fs = df_yearly_max[df_yearly_max[f"{rp_target}yr_target"]][
        "SFED"
    ].min()
    for year, group in df_fs.groupby(df_fs["valid_time"].dt.year):
        dff = group[group["SFED"] >= thresh_fs]
        if not dff.empty:
            dicts.append(
                {
                    "year": year,
                    f"SFED_{rp_target}yr_thresh_date": dff["valid_time"].min(),
                }
            )
    df_in_rp = pd.DataFrame(dicts)
    df_in = df_in.merge(df_in_rp, how="outer")


df_yearly_max = df_yearly_max.merge(df_in, how="left")
```

Clunky way to do this, but determining the first day that the target RP threshold would've been reached (i.e., the day our target flooding starts).

```python
# to avoid divide by zero warnings
np.seterr(divide="ignore", invalid="ignore")
```

### Iterate

Iterate over all possible combinations of GloFAS and Google forecast thresholds. Also iterate over the target RPs.

```python
dff_daily
```

```python
total_years = len(df_yearly_max)
ranks = range(total_years + 1)

dicts = []
dfs_yearly = []

for gf_rank in tqdm(ranks):
    if gf_rank > 0:
        thresh_gf = df_yearly_max.set_index("dis24_rank").loc[gf_rank]["dis24"]
    else:
        thresh_gf = np.inf
    for gr_rank in ranks:
        if gr_rank > 0:
            thresh_gr = df_yearly_max.set_index("streamflow_rank").loc[
                gr_rank
            ]["streamflow"]
        else:
            thresh_gr = np.inf
        dff = df_yearly_max[
            (df_yearly_max["dis24_rank"] <= gf_rank)
            | (df_yearly_max["streamflow_rank"] <= gr_rank)
        ]
        dff_daily = df_compare[
            (df_compare["dis24"] >= thresh_gf)
            | (df_compare["streamflow"] >= thresh_gr)
        ]
        pp_daily = dff_daily["year"].nunique()
        df_trig_dates = (
            dff_daily.groupby("year")["valid_time"]
            .min()
            .reset_index()
            .rename(columns={"valid_time": "trig_date"})
        )
        dff = dff.merge(df_trig_dates, how="left")
        for ind_name, thresh in zip(
            ["dis24", "streamflow"], [thresh_gf, thresh_gr]
        ):
            dff_daily_ind = df_compare[df_compare[ind_name] >= thresh]
            df_trig_dates_ind = (
                dff_daily_ind.groupby("year")["valid_time"]
                .min()
                .reset_index()
                .rename(columns={"valid_time": f"{ind_name}_trig_date"})
            )
            dff = dff.merge(df_trig_dates_ind, how="left")
        dff["leadtime_peak"] = dff["SFED_maxdate"] - dff["trig_date"]
        for rp_target in rp_targets:
            dff[f"leadtime_{rp_target}yr_target"] = (
                dff[f"SFED_{rp_target}yr_thresh_date"] - dff["trig_date"]
            )

            p = df_yearly_max[f"{rp_target}yr_target"].sum()
            pp = len(dff)
            # just double check that the two ways to calculate
            # positives agree
            assert pp_daily == pp
            tp = dff[f"{rp_target}yr_target"].sum()
            fp = pp - tp
            tpr = tp / p
            ppv = tp / (tp + fp)
            f1 = 2 * ppv * tpr / (ppv + tpr)
            dicts.append(
                {
                    "rp_target": rp_target,
                    "rp_gf": np.divide((total_years + 1), gf_rank),
                    "rp_gr": np.divide((total_years + 1), gr_rank),
                    "rp_combined": np.divide((total_years + 1), pp),
                    "tpr": tpr,
                    "lt": dff[f"leadtime_{rp_target}yr_target"].mean(),
                    "f1": f1,
                    "lt_peak": dff[dff[f"{rp_target}yr_target"]][
                        "leadtime_peak"
                    ].mean(),
                }
            )
        dff["rp_gf"] = np.divide((total_years + 1), gf_rank)
        dff["rp_gr"] = np.divide((total_years + 1), gr_rank)
        dfs_yearly.append(dff)

df_rps = pd.DataFrame(dicts)

df_rps
```

```python
# set this to grab values easily later
df_all_simulations = pd.concat(dfs_yearly, ignore_index=True)
```

```python
# set probabilities
for x in ["gr", "gf", "combined"]:
    df_rps[f"prob_{x}"] = 1 / df_rps[f"rp_{x}"]
```

Come up with a measure `rel_prob_gr` that represents the balance between Google and GloFAS. When it is 1, we always trigger with Google (never with GloFAS), and when it is -1, we always trigger with GloFAS (never with Google).

```python
df_rps["rel_prob_gr"] = (df_rps["prob_gr"] - df_rps["prob_gf"]) / df_rps[
    "prob_combined"
]

df_rps
```

## Plots

```python
def plot_metrics_rel_prob(
    rp_target: int,
    plot_metric: str,
    peak_lt: bool = False,
    max_plot_rp: int = 5,
    min_plot_rp: int = 3,
):
    """Plot accuracy metrics and leadtime for multiple overall RPs"""
    lt_col = "lt_peak" if peak_lt else "lt"
    df_plot = df_rps[
        (df_rps["rp_combined"] <= max_plot_rp)
        & (df_rps["rp_combined"] >= min_plot_rp)
        & (df_rps["rp_target"] == rp_target)
    ]

    fig, ax = plt.subplots(dpi=200, figsize=(7, 7))
    ax2 = ax.twinx()

    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for i, (rp_combined, group) in enumerate(df_plot.groupby("rp_combined")):
        group = group.sort_values("rel_prob_gr")
        color = color_cycle[
            i % len(color_cycle)
        ]  # cycle through colors safely

        ax.plot(
            group["rel_prob_gr"],
            group[plot_metric],
            label=f"{rp_combined:.1f}",
            color=color,
            linestyle="-",
        )
        ax2.plot(
            group["rel_prob_gr"],
            group[lt_col].dt.days,
            color=color,
            linestyle="--",
        )

    # Only add legend for rp_combined once (from ax)
    ax.legend(
        title="Overall RP (years)\nsolid: accuracy metric\ndashed: leadtime",
        bbox_to_anchor=(1.1, 1),
        loc="upper left",
    )
    ax.axvline(0, color="lightgrey", linewidth=0.5, linestyle="--")

    # Labels
    ax.set_xlabel(
        "Relative probability of activating\n"
        "<-- more likely with GloFAS              more likely with Google -->"
    )
    ax.set_ylabel(plot_metric.upper())
    lt_label = "peak flooding" if peak_lt else "target flooding threshold"
    ax2.set_ylabel(f"Leadtime to {lt_label} (days)")
    ax.set_xlim([-1, 1])
    ax.set_ylim([0, 1])
    ax2.set_ylim([0, 40])
    ax.set_title(
        f"GloFAS/Google comparison, target {rp_target}-yr RP flooding\n"
        "solid: accruacy metric, dashed: leadtime"
    )
```

```python
plot_metrics_rel_prob(5, "f1", peak_lt=True)
```

```python
plot_metrics_rel_prob(3, "f1", peak_lt=False)
```

```python
plot_metrics_rel_prob(4, "f1", peak_lt=False)
```

```python
plot_metrics_rel_prob(5, "f1", peak_lt=False)
```

### Specific overall RP

Look at only the overall RP specified by CERF (4.5 years).

```python
# df_rps_acceptable = df_rps[
#     (df_rps["rp_combined"] <= 5) & (df_rps["rp_combined"] >= 3)
# ]
acceptable_rp = (total_years + 1) / 6
df_rps_acceptable = df_rps[df_rps["rp_combined"] == acceptable_rp].copy()
```

```python
df_rps_acceptable
```

```python
def plot_metrics_one_overallrp(
    plot_metric: str,
    peak_lt: bool = False,
):
    lt_col = "lt_peak" if peak_lt else "lt"
    df_plot = df_rps_acceptable.copy()

    fig, ax = plt.subplots(dpi=200, figsize=(7, 7))
    ax2 = ax.twinx()

    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for i, (rp_target, group) in enumerate(df_plot.groupby("rp_target")):
        group = group.sort_values("rel_prob_gr")
        color = color_cycle[
            i % len(color_cycle)
        ]  # cycle through colors safely

        ax.plot(
            group["rel_prob_gr"],
            group[plot_metric],
            label=rp_target,
            color=color,
            linestyle="-",
        )
        ax2.plot(
            group["rel_prob_gr"],
            group[lt_col].dt.days,
            color=color,
            linestyle="--",
        )

    # Only add legend for rp_combined once (from ax)
    ax.legend(
        title="Target RP (years)\nsolid: accuracy metric\ndashed: leadtime",
        bbox_to_anchor=(1.1, 1),
        loc="upper left",
    )
    ax.axvline(0, color="lightgrey", linewidth=0.5, linestyle="--")

    # Labels
    ax.set_xlabel(
        "Relative probability of activating\n"
        "<-- more likely with GloFAS              more likely with Google -->"
    )
    ax.set_ylabel(plot_metric.upper())
    lt_label = "peak flooding" if peak_lt else "target flooding threshold"
    ax2.set_ylabel(f"Leadtime to {lt_label} (days)")
    ax.set_xlim([-1, 1])
    ax.set_ylim([0, 1])
    ax2.set_ylim([0, 40])
    ax.set_title(
        f"GloFAS/Google comparison, overall {acceptable_rp:.1f}-yr RP\n"
        "solid: accruacy metric, dashed: leadtime"
    )
```

```python
plot_metrics_one_overallrp("f1")
```

```python
plot_metrics_one_overallrp("tpr")
```

```python
df_rps_acceptable
```

## Specific combination

Pick specification combination based on accuracy metrics. Looks like a balanced trigger (equal probability to activate with both) seems to provide decent combination of accuracy and leadtime. We can then look at the historical performance in more detail.

```python
df_rps_acceptable[df_rps_acceptable["rel_prob_gr"] == 0]
```

```python
df_selected_trigger = df_all_simulations[
    (df_all_simulations["rp_gf"] == 5.4) & (df_all_simulations["rp_gr"] == 5.4)
]
```

```python
df_selected_trigger = df_yearly_max.merge(df_selected_trigger, how="outer")
```

```python
rp_target = 5
```

### True positives

```python
df_selected_trigger
```

```python
df_selected_trigger.columns
```

```python
dff = df_selected_trigger[
    (df_selected_trigger[f"{rp_target}yr_target"])
    & (~df_selected_trigger["rp_gf"].isnull())
]

for _, row in dff.iterrows():
    print(row)
    print()
```

### False positives

```python
dff = df_selected_trigger[
    (~df_selected_trigger[f"{rp_target}yr_target"])
    & (~df_selected_trigger["rp_gf"].isnull())
]

for _, row in dff.iterrows():
    print(row)
    print()
```

### False negatives

```python
dff = df_selected_trigger[
    (df_selected_trigger[f"{rp_target}yr_target"])
    & (df_selected_trigger["rp_gf"].isnull())
]

for _, row in dff.iterrows():
    print(row)
    print()
```

### True negatives

```python
dff = df_selected_trigger[
    (~df_selected_trigger[f"{rp_target}yr_target"])
    & (df_selected_trigger["rp_gf"].isnull())
]

for _, row in dff.iterrows():
    print(row)
    print()
```
