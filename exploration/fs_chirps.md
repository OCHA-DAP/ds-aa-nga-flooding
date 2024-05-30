---
jupyter:
  jupytext:
    formats: md,ipynb
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

# Floodscan vs. CHIRPS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import datetime

import matplotlib.pyplot as plt
import xarray as xr
import cftime
import pandas as pd
import numpy as np
import rioxarray as rxr
import matplotlib.pyplot as plt
from tqdm.notebook import tqdm

from src.datasources import codab, chirps, floodscan
from src.constants import *
```

```python
# floodscan.calculate_adm2_daily_rasterstats()
```

```python
adm = codab.load_codab(admin_level=2, aoi_only=True)
```

```python
fs = floodscan.load_adm2_daily_rasterstats()
rolls = [1, 3, 5, 7, 9, 11]
for roll in rolls:
    fs[f"fs_roll{roll}"] = (
        fs.groupby("ADM2_PCODE")["SFED_AREA"]
        .transform(lambda x: x.rolling(window=roll, min_periods=roll).mean())
        .shift(-np.floor(roll / 2).astype(int))
    )
```

```python
ch = chirps.load_raster_stats()

ch = ch.rename(columns={"T": "time"})
ch = ch.sort_values("time")

rolls = [1, 2, 3, 5, 7, 10, 15, 20, 30, 60, 90]
for roll in rolls:
    ch[f"ch_roll{roll}"] = ch.groupby("ADM2_PCODE")["mean"].transform(
        lambda x: x.rolling(window=roll, min_periods=roll).mean()
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
compare_peak = compare.copy().iloc[
    compare.groupby(["ADM2_PCODE", compare["time"].dt.year])[
        "fs_roll7"
    ].idxmax()
]
compare_peak["year"] = compare_peak["time"].dt.year
```

```python
peak_corr = compare_peak.groupby("ADM2_PCODE").corr()
peak_corr = peak_corr[
    [x for x in peak_corr.columns if "ch_roll" in x]
].reset_index()
peak_corr = peak_corr[peak_corr["level_1"] == "fs_roll7"].drop(
    columns="level_1"
)
peak_corr
```

```python
year = 2022
pcode = JAKUSKO
compare[
    (compare["time"].dt.year == year) & (compare["ADM2_PCODE"] == pcode)
].set_index("time")[["fs_roll1", "fs_roll11"]].plot()
```

```python
ch_rp = 3
fs_rp = 3

roll_fs = 7
roll_ch = 15
fs_col = f"fs_roll{roll_fs}"
ch_col = f"ch_roll{roll_ch}"


def calc_P_PP(group):
    maxes = group.groupby(group["time"].dt.year).max(numeric_only=True)
    group["PP"] = group[ch_col] >= maxes[ch_col].quantile(q=1 - 1 / ch_rp)
    group["P"] = group[fs_col] >= maxes[fs_col].quantile(q=1 - 1 / fs_rp)
    return group


compare = (
    compare.groupby("ADM2_PCODE")
    .apply(calc_P_PP, include_groups=False)
    .reset_index()
    .drop(columns="level_1")
)


def calc_TP(df):
    df["TP"] = df["P"] & df["PP"]
    df["FP"] = df["PP"] & ~df["P"]
    df["FN"] = ~df["PP"] & df["P"]
    df["TN"] = ~df["PP"] & ~df["P"]
    df["N"] = ~df["P"]
    df["PN"] = ~df["PP"]
    return df


compare = calc_TP(compare)


def calc_metrics(df):
    sums = df.sum(numeric_only=True)
    return pd.Series(
        {
            "TPR": sums["TP"] / sums["P"],
            "PPV": sums["TP"] / sums["PP"],
            "TNR": sums["TN"] / sums["N"],
            "FPR": sums["FP"] / sums["N"],
            "FNR": sums["FN"] / sums["P"],
        }
    )


metrics = (
    compare.groupby("ADM2_PCODE")
    .apply(calc_metrics, include_groups=False)
    .reset_index()
)

compare_year = (
    compare.groupby(["ADM2_PCODE", compare["time"].dt.year])[["P", "PP"]]
    .any()
    .reset_index()
)
compare_year = calc_TP(compare_year)

metrics_year = (
    compare_year.groupby("ADM2_PCODE")
    .apply(calc_metrics, include_groups=False)
    .reset_index()
)

metric = "PPV"
fig, ax = plt.subplots(figsize=(8, 8))
adm.merge(metrics, on="ADM2_PCODE").plot(
    column=metric,
    vmin=0,
    vmax=1,
    legend=True,
    # cmap="Blues",
    edgecolor="k",
    linewidth=0.2,
    ax=ax,
)
ax.set_title(
    f"Day-wise {metric} for {fs_rp}-year flood with a {ch_rp}-year trigger\n"
    f"using CHIRPS {roll_ch}-day cumulative rainfall"
)
ax.axis("off")

fig, ax = plt.subplots(figsize=(8, 8))
adm.merge(metrics_year, on="ADM2_PCODE").plot(
    column="TPR",
    vmin=0,
    vmax=1,
    legend=True,
    # cmap="Blues",
    edgecolor="k",
    linewidth=0.2,
    ax=ax,
)
ax.set_title(
    f"Year-wise TPR for {fs_rp}-year flood with a {ch_rp}-year trigger\n"
    f"using CHIRPS {roll_ch}-day cumulative rainfall"
)
ax.axis("off")
```

```python
for pcode in ALL_PRIORITY_ADM2_PCODES:
    print(adm.groupby("ADM2_PCODE").first().loc[pcode, "ADM2_EN"])
    print(metrics_year.set_index("ADM2_PCODE").loc[pcode])
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

fig, ax = plt.subplots(figsize=(8, 8))
adm.merge(corr_max, on="ADM2_PCODE").plot(
    column="corr",
    vmin=0,
    legend=True,
    cmap="Purples",
    edgecolor="k",
    linewidth=0.2,
    ax=ax,
)
ax.axis("off")
```

```python
for ch_roll, group in corr.groupby("ch_roll"):
    fig, ax = plt.subplots(figsize=(8, 8))
    adm.merge(group, on="ADM2_PCODE").plot(
        column="corr",
        vmin=0,
        vmax=1,
        legend=True,
        cmap="Purples",
        edgecolor="k",
        linewidth=0.2,
        ax=ax,
    )
    ax.axis("off")
    ax.set_title(
        f"Correlation of flood extent with {ch_roll}-day cumulative rainfall"
    )
```

```python

```

```python
pcode = NGALA
admin2_name = adm[adm["ADM2_PCODE"] == pcode]["ADM2_EN"].iloc[0]
roll_fs = 7
roll_ch = 15
fs_max = compare[compare["ADM2_PCODE"] == pcode][f"fs_roll{roll_fs}"].max()
ch_max = compare[compare["ADM2_PCODE"] == pcode][f"ch_roll{roll_ch}"].max()


# for year in compare["time"].dt.year.unique():
for year in [2022]:
    compare_f = compare[
        (compare["time"].dt.year == year) & (compare["ADM2_PCODE"] == pcode)
    ]
    compare_seasonal_f = compare_seasonal[
        compare_seasonal["ADM2_PCODE"] == pcode
    ].copy()

    fig, axs = plt.subplots(2, 1, sharex=True, figsize=(10, 6))
    compare_f.plot(x="time", y=f"fs_roll{roll_fs}", label=year, ax=axs[0])
    compare_seasonal_f["date_dummy"] = pd.Timestamp(
        f"{year}-01-01"
    ) + pd.to_timedelta(compare_seasonal_f["dayofyear"] - 1, unit="d")

    compare_seasonal_f.plot(
        x="date_dummy",
        y=f"fs_roll{roll_fs}",
        ax=axs[0],
        label="mean (1998-2023)",
    )
    for index, row in compare_f.set_index("time").iterrows():
        if row["P"]:
            axs[0].axvspan(
                index, index + pd.Timedelta(days=1), color="red", alpha=0.2
            )
        if row["PP"]:
            axs[1].axvspan(
                index, index + pd.Timedelta(days=1), color="red", alpha=0.2
            )

    axs[0].set_ylabel(f"Flooded fraction\n{roll_fs}-day rolling average")
    axs[0].set_ylim(bottom=0, top=fs_max)

    compare_f.plot(
        x="time", y=f"ch_roll{roll_ch}", ax=axs[1], label=year, legend=False
    )

    compare_seasonal_f.plot(
        x="date_dummy",
        y=f"ch_roll{roll_ch}",
        ax=axs[1],
        label="mean (1998-2023)",
        legend=False,
    )

    axs[1].set_ylabel(
        f"Daily precipitation\n{roll_ch}-day rolling average (mm)"
    )
    axs[1].set_ylim(bottom=0, top=ch_max)

    for ax in axs:
        ax.set_xlabel("Date")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    axs[0].set_title(f"LGA: {admin2_name}, Year: {year}")
```

```python
compare_f
```
