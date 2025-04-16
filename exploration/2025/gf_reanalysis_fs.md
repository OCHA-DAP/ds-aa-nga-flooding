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

# GloFAS reanalysis / Floodscan
<!-- markdownlint-disable MD013 -->
Validation of GloFAS reanalysis using Floodscan in relevant LGAs or pixels

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import ocha_stratus as stratus
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
import matplotlib.colors as mcolors

from src.datasources import glofas, codab
from src.constants import *
from src.utils import blob
```

## Load data

```python
station_name = "wuroboki"
```

```python
df_ra = glofas.load_glofas_reanalysis(station_name=station_name)
```

```python
df_ra
```

```python
adm2 = codab.load_codab_from_blob(admin_level=2, aoi_only=True)
```

```python
pcodes = BENUE_ADM2_PCODES
```

```python
query = f"""
SELECT *
FROM public.floodscan
WHERE pcode IN {tuple(pcodes)}
"""
```

```python
df_fs = pd.read_sql(
    query, stratus.get_engine(stage="prod"), parse_dates=["valid_date"]
)
```

```python
df_sfed = df_fs[df_fs["band"] == "SFED"]
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/floodscan/fs_benue_pixels_1998_2024.parquet"
df_fs_pixels = stratus.load_parquet_from_blob(blob_name)
```

```python
df_fs_pixels["pixel_id"] = (
    df_fs_pixels["x"].astype(str) + "_" + df_fs_pixels["y"].astype(str)
)
```

```python
df_fs_pixels
```

## Compare pixels

See which pixels have the best correlation with GloFAS reanalysis

```python
df_compare_pixels = df_ra.rename(columns={"time": "date"}).merge(df_fs_pixels)
```

```python
dicts = []
for (x, y), group in df_compare_pixels.groupby(["x", "y"]):
    dicts.append(
        {
            "x": x,
            "y": y,
            "corr": group.corr(numeric_only=True).loc["dis24", "SFED"],
        }
    )
```

```python
df_compare_pixels.groupby("date")[["SFED", "dis24"]].mean().corr()
```

```python
df_corr_pixels = pd.DataFrame(dicts).sort_values("corr", ascending=False)
```

```python
df_corr_pixels.set_index(["y", "x"]).to_xarray()["corr"].plot()
```

```python
df_corr_pixels.sort_values("corr", ascending=False)
```

```python
top_x, top_y = df_corr_pixels.sort_values("corr", ascending=False).iloc[1][
    ["x", "y"]
]
```

## Combine reanalysis with Floodscan

There are a few different ways we can select with Floodscan pixels to use in the comparison.

Run only the relevant cells to merge that Floodscan aggregation for comparison.

### 1. Use admin2 Floodscan raster stats

```python
df_compare = (
    df_ra.rename(columns={"time": "valid_date"})
    .merge(df_sfed.rename(columns={"pcode": "ADM2_PCODE"}))
    .merge(adm2[["ADM2_PCODE", "ADM2_EN"]])
)
```

```python
for name, group in df_compare.groupby("ADM2_EN"):
    print(name)
    display(group.corr(numeric_only=True).loc["dis24", "mean"])
```

```python
df_compare_adm2 = df_compare[df_compare["ADM2_PCODE"] == NUMAN2].copy()
```

### 2. Use mean of pixels

```python
df_compare_adm2 = (
    df_compare_pixels.groupby("date")[["dis24", "SFED"]]
    .mean()
    .reset_index()
    .copy()
    .rename(columns={"date": "valid_date", "SFED": "mean"})
)
```

### 3. Use the best pixel

```python
df_compare_adm2 = (
    df_compare_pixels[
        (df_compare_pixels["x"] == top_x) & (df_compare_pixels["y"] == top_y)
    ]
    .copy()
    .rename(columns={"date": "valid_date", "SFED": "mean"})
)
```

## Further processing

```python
df_compare_adm2["mean"] = (
    df_compare_adm2["mean"].rolling(7, center=True).mean()
)
```

```python
df_compare_adm2["year"] = df_compare_adm2["valid_date"].dt.year

df_compare_adm2 = df_compare_adm2[df_compare_adm2["year"] >= 2003]
```

```python
df_plot = df_compare_adm2[df_compare_adm2["valid_date"].dt.year == 2022].copy()
df_plot.plot(x="valid_date", y="mean")
```

```python
df_plot.loc[df_plot["mean"].idxmax()]
```

```python
df_compare_adm2[df_compare_adm2["valid_date"].dt.year == 2022]
```

### Calculate peaks

```python
# For dis24
dis24_max_idx = df_compare_adm2.groupby("year")["dis24"].idxmax()
dis24_max_df = df_compare_adm2.loc[
    dis24_max_idx, ["year", "valid_date", "dis24"]
].rename(columns={"valid_date": "rea_date", "dis24": "rea_max"})

# For mean
mean_max_idx = df_compare_adm2.groupby("year")["mean"].idxmax()
mean_max_df = df_compare_adm2.loc[
    mean_max_idx, ["year", "valid_date", "mean"]
].rename(columns={"valid_date": "fs_date", "mean": "fs_max"})

# Merge the two results
df_peaks = pd.merge(dis24_max_df, mean_max_df, on="year", how="outer")
```

```python
for x in ["rea", "fs"]:
    df_peaks[f"{x}_date_doy"] = pd.to_datetime(
        df_peaks[f"{x}_date"].dt.dayofyear, format="%j"
    )
df_peaks
```

## Plotting

### Peak accuracy

```python
rea_color = "royalblue"
fs_color = "crimson"
both_color = "purple"
none_color = "grey"
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(8, 8))

xmin, ymin = df_peaks[["rea_max", "fs_max"]].min()
xmax, ymax = df_peaks[["rea_max", "fs_max"]].max()
xrange = xmax - xmin
yrange = ymax - ymin
pad = 0.05
xlims = (xmin - xrange * pad, xmax + xrange * pad)
ylims = (ymin - yrange * pad, ymax + yrange * pad)

rp = 3

rea_thresh = df_peaks["rea_max"].quantile(1 - 1 / rp)
fs_thresh = df_peaks["fs_max"].quantile(1 - 1 / rp)

ax.axvline(rea_thresh, color=rea_color, linewidth=1)
ax.axhline(fs_thresh, color=fs_color, linewidth=1)

ax.axhspan(fs_thresh, 2, color=fs_color, alpha=0.1)
ax.axvspan(rea_thresh, 10000, color=rea_color, alpha=0.1)

for year, row in df_peaks.set_index("year").iterrows():
    if (row["rea_max"] >= rea_thresh) & (row["fs_max"] >= fs_thresh):
        color = both_color
    elif row["rea_max"] >= rea_thresh:
        color = rea_color
    elif row["fs_max"] >= fs_thresh:
        color = fs_color
    else:
        color = none_color
    ax.annotate(
        year,
        (row["rea_max"], row["fs_max"]),
        ha="center",
        va="center",
        fontsize=8,
        color=color,
    )

ax.set_xlim(xlims)
ax.set_ylim(ylims)

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

ax.set_xlabel("GloFAS reanalysis yearly peak")
ax.set_ylabel("Floodscan yearly peak")
```

### Peak timing

```python
fig, ax = plt.subplots(dpi=200, figsize=(8, 8))

rp = 3

xmin = df_peaks[["rea_date_doy", "fs_date_doy"]].min().min()
xmax = df_peaks[["rea_date_doy", "fs_date_doy"]].max().max()
xrange = xmax - xmin
pad = 0.05
lims = (xmin - xrange * pad, xmax + xrange * pad)

rea_thresh = df_peaks["rea_max"].quantile(1 - 1 / rp)
fs_thresh = df_peaks["fs_max"].quantile(1 - 1 / rp)

dates_45 = pd.to_datetime(["1900-01-01", "1900-12-31"])

ax.plot(
    dates_45,
    dates_45,
    linewidth=1,
    color="grey",
    linestyle="--",
)

alpha = 0.2

days_early_list = range(-50, 41, 10)
norm = mcolors.Normalize(vmin=min(days_early_list), vmax=max(days_early_list))
cmap = cm.get_cmap("BrBG")

for days_early in days_early_list:
    if days_early == max(days_early_list):
        upper_dates = 0
    else:
        upper_dates = dates_45 + pd.DateOffset(days=days_early + 10)
    if days_early == min(days_early_list):
        lower_dates = dates_45 + pd.DateOffset(days=-1000)
    else:
        lower_dates = dates_45 + pd.DateOffset(days=days_early)
    ax.fill_between(
        dates_45,
        lower_dates,
        upper_dates,
        facecolor=cmap(norm(days_early)),
        alpha=alpha,
    )
    if days_early < 0:
        annotation = (
            f"{-days_early-10}+ days late"
            if days_early == min(days_early_list)
            else f"{-days_early-10}-{-days_early} days late"
        )
    else:
        annotation = (
            f"{days_early}+ days early"
            if days_early == max(days_early_list)
            else f"{days_early}-{days_early+10} days early"
        )
    ax.annotate(
        annotation,
        (
            pd.to_datetime("1900-08-10")
            - pd.DateOffset(days=days_early / 2 + 5),
            pd.to_datetime("1900-08-10") + pd.DateOffset(days=days_early / 2),
        ),
        rotation=45,
        ha="center",
        va="center",
        fontsize=8,
        fontstyle="italic",
        color="grey",
    )

for year, row in df_peaks.set_index("year").iterrows():
    if (row["rea_max"] >= rea_thresh) & (row["fs_max"] >= fs_thresh):
        color = both_color
    elif row["rea_max"] >= rea_thresh:
        color = rea_color
    elif row["fs_max"] >= fs_thresh:
        color = fs_color
    else:
        color = none_color
    ax.annotate(
        year,
        (row["rea_date_doy"], row["fs_date_doy"]),
        ha="center",
        va="center",
        fontsize=8,
        color=color,
    )

ax.set_xlim(lims)
ax.set_ylim(lims)

ax.set_xlabel("GloFAS reanalysis peak date")
ax.set_ylabel("Floodscan peak date")

ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b"))
ax.yaxis.set_major_formatter(mdates.DateFormatter("%d-%b"))

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
```

```python
df_peaks
```

### Timeseries

```python
dis_max, fs_max = df_compare_adm2[["dis24", "mean"]].max()

for year, group in df_compare_adm2.groupby("year"):
    fig, ax1 = plt.subplots()

    # Plot dis24 on the primary y-axis
    ax1.plot(
        group["valid_date"], group["dis24"], color="tab:blue", label="dis24"
    )
    ax1.set_ylabel("dis24", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.set_ylim(bottom=0, top=dis_max)

    # Create a second y-axis
    ax2 = ax1.twinx()
    ax2.plot(group["valid_date"], group["mean"], color="tab:red", label="mean")
    ax2.set_ylabel("mean", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.set_ylim(bottom=0, top=fs_max)
```
