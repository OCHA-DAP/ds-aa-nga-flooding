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

# GloFAS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import datetime

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import glofas, floodscan
from src.constants import *
```

```python
BENUE_ADM2_PCODES = ["NG002016", "NG002009", "NG002021", "NG002005"]
fs = floodscan.load_adm2_daily_rasterstats()
fs = fs[fs["ADM2_PCODE"].isin(BENUE_ADM2_PCODES)]
```

```python
fs_mean = fs.groupby(fs["time"].dt.year)["SFED_AREA"].mean().reset_index()
fs_mean = fs_mean.rename(columns={"time": "year"})
```

```python
# glofas.process_reanalysis()
```

```python
# glofas.download_reforecast()
```

```python
# glofas.process_reforecast()
```

```python
ref = glofas.load_reforecast()
rea = glofas.load_reanalysis()
rea = rea[rea["time"].dt.year.isin(ref["time"].dt.year.unique())]
```

```python
rp_f = 3
rp_a = 5

dfs = []

for lt in ref["leadtime"].unique():
    dff = ref[ref["leadtime"] <= lt]
    df_in = dff.loc[dff.groupby(dff["time"].dt.year)["dis24"].idxmax()]
    df_in["lt_max"] = lt
    thresh = df_in["dis24"].quantile(1 - 1 / rp_f)
    df_in["trigger"] = df_in["dis24"] > thresh
    print(lt, thresh)
    dfs.append(df_in)

ref_peaks = pd.concat(dfs, ignore_index=True)
ref_peaks["year"] = ref_peaks["time"].dt.year
```

```python
ref_peaks
```

```python
rea_peaks = rea.loc[rea.groupby(rea["time"].dt.year)["dis24"].idxmax()]
q = rea_peaks["dis24"].quantile(1 - 1 / rp_a)
rea_peaks["trigger"] = rea_peaks["dis24"] > q
rea_peaks["year"] = rea_peaks["time"].dt.year
rea_peaks["cerf"] = rea_peaks["year"].isin(CERF_YEARS)
```

```python
rea_peaks["rank"] = rea_peaks["dis24"].rank(ascending=False)
rea_peaks["rp"] = len(rea_peaks) / rea_peaks["rank"]
rea_peaks = rea_peaks.sort_values("rank", ascending=False)
```

```python
rea_peaks
```

```python
fig, ax = plt.subplots(dpi=300)
rea_peaks.plot(x="rp", y="dis24", ax=ax, legend=False, color="dodgerblue")

for year in rea_peaks.iloc[-8:]["year"]:
    x, y = rea_peaks.set_index("year").loc[year][["rp", "dis24"]]
    ax.plot(
        x,
        y,
        marker=".",
        color="grey",
    )
    ax.annotate(year, (x, y), ha="right", color="grey", fontsize=8)

annotation_x = 13

for rp in [1.5, 2, 3, 4]:
    y = np.interp(
        rp,
        rea_peaks["rp"],
        rea_peaks["dis24"],
    )
    ax.plot(
        [rp, annotation_x],
        [y, y],
        color="crimson",
        linestyle="--",
        linewidth=1,
    )
    ax.annotate(
        f"{rp}-year RP level = {y:,.0f} m^3 / s",
        (annotation_x, y),
        fontsize=8,
        color="crimson",
        va="center",
    )

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_xlim(left=1)
# ax.set_ylim(top=3500)
ax.set_xlabel("Return period (years)")
ax.set_ylabel("Maximum yearly flowrate (m^3 / s)")
ax.set_title("Benue river at Wuroboki\nGloFAS reanalysis (2003-2022)")
```

```python
compare = rea_peaks.merge(ref_peaks, on="year", suffixes=["_a", "_f"]).merge(
    fs_mean
)
```

```python
for indicator in ["cerf", "trigger_a"]:
    compare[f"TP_{indicator}"] = compare[indicator] & compare["trigger_f"]
    compare[f"FP_{indicator}"] = ~compare[indicator] & compare["trigger_f"]
    compare[f"TN_{indicator}"] = ~compare[indicator] & ~compare["trigger_f"]
    compare[f"FN_{indicator}"] = compare[indicator] & ~compare["trigger_f"]
```

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
```

```python
metrics
```

```python
rp_a_3 = 2600
rp_a_5 = 3000
rp_f = 2911.802066666667
compare_lt = compare[compare["lt_max"] == 7]
fig, ax = plt.subplots(dpi=300)
compare_lt.plot(
    y="dis24_a",
    x="dis24_f",
    ax=ax,
    marker=".",
    color="k",
    linestyle="",
    legend=False,
)

ax.axvline(x=rp_f, color="dodgerblue", linestyle="-", linewidth=0.3)
ax.axvspan(
    rp_f,
    6000,
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

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_ylabel("Reanalysis")
ax.set_xlabel("Forecast (up to 7 day leadtime)")
ax.set_ylim(top=5500)
ax.set_xlim(right=5600)
ax.set_title("Benue river at Wuroboki\nGloFAS yearly peaks (2003-2022)")
```

```python
compare_lt[compare["trigger_f"]]
```

```python
rp_a_3 = 2600
rp_a_5 = 3000
rp_f = 2911.802066666667
compare_lt = compare[compare["lt_max"] == 7]
fig, ax = plt.subplots(dpi=300)
compare_lt.plot(
    y="SFED_AREA",
    x="dis24_f",
    ax=ax,
    marker=".",
    color="k",
    linestyle="",
    legend=False,
)

# ax.axvline(x=rp_f, color="dodgerblue", linestyle="-", linewidth=0.3)
# ax.axvspan(
#     rp_f,
#     6000,
#     ymin=0,
#     ymax=1,
#     color="dodgerblue",
#     alpha=0.1,
# )

# ax.axhline(y=rp_a_3, color="red", linestyle="-", linewidth=0.3)
# ax.axhspan(
#     rp_a_3,
#     6000,
#     color="red",
#     alpha=0.05,
#     linestyle="None",
# )

# ax.axhline(y=rp_a_5, color="red", linestyle="-", linewidth=0.3)
# ax.axhspan(
#     rp_a_5,
#     6000,
#     color="red",
#     alpha=0.05,
#     linestyle="None",
# )

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_ylabel("Reanalysis")
ax.set_xlabel("Forecast (up to 7 day leadtime)")
# ax.set_ylim(top=5500)
# ax.set_xlim(right=5600)
ax.set_title("Benue river at Wuroboki\nGloFAS yearly peaks (2003-2022)")
```

```python
compare[compare["lt_max"] == 1]
```

```python
rea[rea["time"].dt.year == 2014].plot(x="time", y="dis24")
```

```python
ref[(ref["time"].dt.year == 2014) & (ref["leadtime"] == 1)].plot(
    x="time", y="dis24"
)
```

```python

```
