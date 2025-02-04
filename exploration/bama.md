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

# Bama and Dikwa

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from src.datasources import codab, chirps, floodscan
from src.constants import *
```

```python
ADM2_PCODES = [BAMA, DIKWA, NGALA]
```

```python
# codab.download_codab_to_blob()
```

```python
adm = codab.load_codab_from_blob(admin_level=2)
adm_borno = adm[adm["ADM1_PCODE"] == BORNO]
adm_aoi = adm[adm["ADM2_PCODE"].isin(ADM2_PCODES)]
```

```python
fig, ax = plt.subplots(dpi=300)
adm_aoi.plot(column="ADM2_PCODE", ax=ax)
adm_borno.boundary.plot(ax=ax, linewidth=0.5, color="k")
ax.axis("off")
for _, row in adm_aoi.iterrows():
    ax.annotate(
        row["ADM2_EN"],
        (row.geometry.centroid.x, row.geometry.centroid.y),
        ha="center",
        va="center",
        fontsize=6,
    )
```

```python
fs = floodscan.load_adm2_daily_rasterstats()
fs = fs[fs["ADM2_PCODE"].isin(ADM2_PCODES)]
rolls = [1, 3, 5, 7, 9, 11]
for roll in rolls:
    fs[f"fs_roll{roll}"] = (
        fs.groupby("ADM2_PCODE")["SFED_AREA"]
        .transform(lambda x: x.rolling(window=roll, min_periods=roll).mean())
        .shift(-np.floor(roll / 2).astype(int))
    )
```

```python
fs
```

```python
exposure = floodscan.load_adm2_flood_exposures()
exposure = exposure[exposure["ADM2_PCODE"].isin(ADM2_PCODES)]
```

```python
ch = chirps.load_raster_stats()
ch = ch[ch["ADM2_PCODE"].isin(ADM2_PCODES)]

ch = ch.rename(columns={"T": "time"})
ch = ch.sort_values("time")

rolls = [1, 2, 3, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
for roll in rolls:
    ch[f"ch_roll{roll}"] = ch.groupby("ADM2_PCODE")["mean"].transform(
        lambda x: x.rolling(window=roll, min_periods=roll).sum()
    )
```

```python
ch
```

```python
n_dayss = [15, 30, 15]

for pcode, n_days in zip(ADM2_PCODES, n_dayss):
    ch_f = ch[ch["ADM2_PCODE"] == pcode]
    ch_year = (
        ch_f.groupby(ch["time"].dt.year)[f"ch_roll{n_days}"]
        .max()
        .reset_index()
    )
    ch_year["rank"] = ch_year[f"ch_roll{n_days}"].rank()
    ch_year["rp"] = len(ch_year) / (len(ch_year) - ch_year["rank"] + 1)
    ch_year = ch_year.sort_values("rp", ascending=True)

    adm_name = adm.groupby("ADM2_PCODE").first().loc[pcode, "ADM2_EN"]

    fig, ax = plt.subplots(dpi=300)
    ch_year.plot(
        x="rp",
        y=f"ch_roll{n_days}",
        ax=ax,
        legend=False,
        linewidth=1,
        color="dodgerblue",
    )

    for year in ch_year.iloc[-6:]["time"]:
        x, y = ch_year.set_index("time").loc[year][["rp", f"ch_roll{n_days}"]]
        ax.plot(
            x,
            y,
            marker=".",
            color="grey",
        )
        ax.annotate(year, (x, y), ha="right", color="grey", fontsize=8)

    annotation_x = 15

    for rp in [1.5, 2, 3, 4]:
        y = np.interp(
            rp,
            ch_year["rp"],
            ch_year[f"ch_roll{n_days}"],
        )
        ax.plot(
            [1, annotation_x],
            [y, y],
            color="crimson",
            linestyle="--",
            linewidth=1,
        )
        ax.annotate(
            f"{rp}-year RP level = {y:.0f} mm",
            (annotation_x, y),
            fontsize=8,
            color="crimson",
            va="center",
        )

    ax.set_xlabel("Return period (years)")
    ax.set_ylabel(f"Maximum {n_days}-day accumulated rainfall (mm)")
    ax.set_xlim(left=1)
    ax.set_title(f"{adm_name} {n_days}-day rainfall return period")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    rp = 3
    thresh = np.interp(
        rp,
        ch_year["rp"],
        ch_year[f"ch_roll{n_days}"],
    )
    print(thresh)
    ch_trig = ch_f[ch_f[f"ch_roll{n_days}"] >= thresh]
    print(adm_name)
    display(
        ch_trig.groupby(ch_trig["time"].dt.year)["time"]
        .min()
        .rename("Trigger Date")
        .reset_index()
        .rename(columns={"time": "Year"})
    )
```

```python
ch_seasonal = (
    ch.groupby(["ADM2_PCODE", ch["time"].dt.dayofyear])
    .mean()
    .drop(columns="time")
    .reset_index()
    .rename(columns={"time": "dayofyear"})
)
ch_seasonal["date_2000"] = pd.Timestamp("2000-01-01") + pd.to_timedelta(
    ch_seasonal["dayofyear"] - 1, unit="d"
)
```

```python
compare = fs.merge(ch, on=["ADM2_PCODE", "time"])
compare_seasonal = (
    compare.groupby(["ADM2_PCODE", compare["time"].dt.dayofyear])
    .mean()
    .drop(columns="time")
    .reset_index()
    .rename(columns={"time": "dayofyear"})
)
compare_seasonal["date_2000"] = pd.Timestamp("2000-01-01") + pd.to_timedelta(
    compare_seasonal["dayofyear"] - 1, unit="d"
)
```

```python
fs_roll = 7

corr = compare.groupby("ADM2_PCODE").corr()
corr = (
    corr[[x for x in corr.columns if "fs_roll" in x]]
    .reset_index()
    .rename(columns={"level_1": "ch_roll"})
)
corr = corr[corr["ch_roll"].str.contains("ch_roll")]
corr = corr.melt(
    id_vars=["ADM2_PCODE", "ch_roll"],
    value_vars=[x for x in corr.columns if "fs_roll" in x],
    value_name="corr",
    var_name="fs_roll",
)
corr["ch_roll"] = corr["ch_roll"].str.removeprefix("ch_roll").astype(int)
corr["fs_roll"] = corr["fs_roll"].str.removeprefix("fs_roll").astype(int)
corr = corr[corr["fs_roll"] == fs_roll]

ch_roll_max = 30
corr_max = corr.loc[
    corr[corr["ch_roll"] <= ch_roll_max].groupby("ADM2_PCODE")["corr"].idxmax()
]
corr_max = corr_max.sort_values("corr", ascending=False)
```

```python
for pcode in ADM2_PCODES:
    adm_name = adm.groupby("ADM2_PCODE").first().loc[pcode, "ADM2_EN"]
    fig, ax = plt.subplots(dpi=300)
    corr[corr["ADM2_PCODE"] == pcode].plot(
        x="ch_roll", y="corr", ax=ax, legend=False
    )
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xlabel("Number of days to accumulate precipitation")
    ax.set_ylabel("Correlation with flood extent")
    ax.set_title(f"{adm_name} rainfall-flood extent correlation")
```

```python
corr
```

```python
corr.loc[corr["corr"].idxmax()]
```
