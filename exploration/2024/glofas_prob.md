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

# GloFAS probabilistic

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os

import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from tqdm.notebook import tqdm

from src.utils import blob
from src.datasources import glofas
from src.constants import *
```

```python
# glofas.process_reforecast_frac()
```

```python
ref = glofas.load_reforecast_frac()
```

```python
ref["time"].dt.year.nunique()
```

## Calculate reanalysis peaks

```python
rp_a = 3

rea = glofas.load_reanalysis()
rea = rea[rea["time"].dt.year.isin(ref["time"].dt.year.unique())]
rea_peaks = rea.loc[rea.groupby(rea["time"].dt.year)["dis24"].idxmax()]
q = rea_peaks["dis24"].quantile(1 - 1 / rp_a)
rea_peaks["trigger"] = rea_peaks["dis24"] > q
rea_peaks["year"] = rea_peaks["time"].dt.year
rea_peaks["cerf"] = rea_peaks["year"].isin(CERF_YEARS)
rea_peaks["rank"] = rea_peaks["dis24"].rank(ascending=False)
rea_peaks["rp"] = len(rea_peaks) / rea_peaks["rank"]
rea_peaks = rea_peaks.sort_values("rank", ascending=False)
```

```python
rea_peaks
```

```python
ref
```

## Calculate reforecast peaks

Filtering by maximum leadtime

```python
rp_f = 3
lt_min = 7

val_col = "2yr_thresh"

dfs = []

for lt in ref["leadtime"].unique():
    if lt < lt_min or lt > 30:
        continue

    dff = ref[(ref["leadtime"] <= lt) & (ref["leadtime"] >= lt_min)]
    df_in = dff.loc[dff.groupby(dff["time"].dt.year)[val_col].idxmax()]
    df_in["lt_max"] = lt
    thresh = df_in[val_col].quantile(1 - 1 / rp_f)
    df_in["trigger"] = df_in[val_col] >= thresh
    print(lt, thresh)
    dfs.append(df_in)

ref_peaks = pd.concat(dfs, ignore_index=True)
ref_peaks["year"] = ref_peaks["time"].dt.year
```

## Compare reanalysis to reforecast

### Calcuate TP/FP/TN/FN

```python
compare = rea_peaks.merge(ref_peaks, on="year", suffixes=["_a", "_f"])
for indicator in ["cerf", "trigger_a"]:
    compare[f"TP_{indicator}"] = compare[indicator] & compare["trigger_f"]
    compare[f"FP_{indicator}"] = ~compare[indicator] & compare["trigger_f"]
    compare[f"TN_{indicator}"] = ~compare[indicator] & ~compare["trigger_f"]
    compare[f"FN_{indicator}"] = compare[indicator] & ~compare["trigger_f"]

compare = compare.sort_values(["year", "lt_max"])
```

### Calculate TPR etc metrics

```python
dicts = []
for lt, group in compare.groupby("lt_max"):
    TPR = group["TP_trigger_a"].sum() / group["trigger_a"].sum()
    PPV = group["TP_trigger_a"].sum() / group["trigger_f"].sum()
    TPR_C = group["TP_cerf"].sum() / group["cerf"].sum()
    PPV_C = group["TP_cerf"].sum() / group["trigger_f"].sum()
    dicts.append(
        {"TPR": TPR, "PPV": PPV, "TPR_C": TPR_C, "PPV_C": PPV_C, "lt_max": lt}
    )

metrics = pd.DataFrame(dicts)
metrics
```

```python
compare[compare["trigger_f"]].set_index("lt_max").loc[14]
```

### Plot

For chosen max leadtime and return period

```python
# 3yr
rp_a_3 = 2600
# 2yr
# rp_a_3 = 2364.7734
rp_a_5 = 3109
rp_f = 0.8 * 100
compare_lt = compare[compare["lt_max"] == 14]
compare_lt["percent"] = compare_lt[val_col] * 100
fig, ax = plt.subplots(dpi=300)
compare_lt.plot(
    y="dis24",
    x="percent",
    ax=ax,
    marker=".",
    color="k",
    linestyle="",
    legend=False,
)

ax.axvline(x=rp_f, color="dodgerblue", linestyle="-", linewidth=0.3)
ax.axvspan(
    rp_f,
    100,
    ymin=0,
    ymax=1,
    color="dodgerblue",
    alpha=0.1,
)

ax.axhline(y=rp_a_3, color="red", linestyle="-", linewidth=0.3)
ax.axhspan(
    rp_a_3,
    6000,
    color="red",
    alpha=0.05,
    linestyle="None",
)

ax.axhline(y=rp_a_5, color="red", linestyle="-", linewidth=0.3)
ax.axhspan(
    rp_a_5,
    6000,
    color="red",
    alpha=0.05,
    linestyle="None",
)

for year, row in compare_lt.set_index("year").iterrows():
    flip_years = [2017, 2010]
    ha = "right" if year in flip_years else "left"
    ax.annotate(
        f" {year} ",
        (row["percent"], row["dis24"]),
        color="k",
        fontsize=8,
        va="center",
        ha=ha,
    )

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_ylabel("Reanalysis (m$^3$/s)")
ax.set_xlabel(f"Forecast (% above 2 yr RP, leadtime {lt_min}-14 days)")
ax.set_ylim(top=5500)
ax.set_xlim(right=100)
ax.set_title("Benue river at Wuroboki\nGloFAS yearly peaks (2003-2022)")
```

```python
cols = ["year", "time_f", "leadtime"]
compare_lt[compare_lt["trigger_f"]][cols].rename(
    columns={
        "year": "Year",
        "time_f": "Trigger date",
        "leadtime": "Leadtime (days)",
    }
)
```

```python
dicts = []
for lt_max, group in compare.groupby("lt_max"):
    corr_in = group.corr()
    dicts.append(
        {
            "lt_max": lt_max,
            "2yr_thresh": corr_in.loc["dis24", "2yr_thresh"],
            "5yr_thresh": corr_in.loc["dis24", "5yr_thresh"],
        }
    )

df_corr = pd.DataFrame(dicts)
df_corr
```

```python
df_corr.set_index("lt_max").plot()
```

```python

```
