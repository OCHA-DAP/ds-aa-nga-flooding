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

# Trigger plots

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import numpy as np

from src.datasources import grrr, glofas
from src.constants import *
```

```python
gf_thresh = 3132
gf_lt_max = 5
grrr_thresh = 1195
```

## Load and process data

### GloFAS reforecast

```python
df_ref_ens = pd.read_parquet(
    glofas.GF_PROC_DIR / "wuroboki_glofas_reforecast_ens.parquet"
)
```

```python
df_gf_ref = (
    df_ref_ens.groupby(["valid_time", "leadtime"])["dis24"]
    .mean()
    .reset_index()
)
```

```python
df_gf_ref["valid_time"].max()
```

```python
df_gf_ref_peaks = (
    df_gf_ref.groupby([df_gf_ref["valid_time"].dt.year, "leadtime"])["dis24"]
    .max()
    .reset_index()
    .rename(columns={"valid_time": "year", "dis24": "dis24_f"})
)
```

```python
df_gf_ref_peaks_any_lt = (
    df_gf_ref_peaks[df_gf_ref_peaks["leadtime"] <= gf_lt_max]
    .groupby("year")["dis24_f"]
    .max()
    .reset_index()
)
```

```python
df_gf_ref_peaks_any_lt
```

### GloFAS reanalysis

```python
df_gf_rea = glofas.load_glofas_reanalysis(station_name="wuroboki")
```

```python
df_gf_rea["time"].max()
```

```python
df_gf_rea_peaks = (
    df_gf_rea.groupby(df_gf_rea["time"].dt.year)["dis24"]
    .max()
    .reset_index()
    .rename(columns={"time": "year", "dis24": "dis24_a"})
)
```

```python
df_gf_rea_peaks
```

### Google forecast

```python
ds_rf = grrr.load_reforecast()
df_grrr_ref = grrr.process_reforecast(ds_rf)
```

```python
df_grrr_ref["valid_time"].max()
```

```python
df_grrr_ref_peaks = (
    df_grrr_ref[df_grrr_ref["valid_time"].dt.year < 2023]
    .groupby([df_grrr_ref["valid_time"].dt.year, "leadtime"])["streamflow"]
    .max()
    .reset_index()
    .rename(columns={"valid_time": "year", "streamflow": "streamflow_f"})
)
```

```python
df_grrr_ref_peaks
```

```python
df_grrr_ref_peaks_any_lt = (
    df_grrr_ref_peaks.groupby("year")["streamflow_f"].max().reset_index()
)
```

```python
df_grrr_ref_peaks_any_lt
```

### Google reanalysis

```python
ds_ra = grrr.load_reanalysis()
df_grrr_rea = grrr.process_reanalysis(ds_ra)
```

```python
df_grrr_rea["valid_time"].max()
```

```python
df_grrr_rea_peaks = (
    df_grrr_rea.groupby(df_grrr_rea["valid_time"].dt.year)["streamflow"]
    .max()
    .reset_index()
    .rename(columns={"valid_time": "year", "streamflow": "streamflow_a"})
)
```

```python
df_grrr_rea_peaks
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
df_fs_peaks = (
    df_fs.groupby(df_fs["valid_time"].dt.year)["SFED"].max().reset_index()
)
```

```python
df_fs_peaks = df_fs_peaks.rename(columns={"valid_time": "year"})
df_fs_peaks
```

## Combine

```python
df_compare = (
    df_fs_peaks.merge(df_gf_rea_peaks, how="outer")
    .merge(df_gf_ref_peaks_any_lt, how="outer")
    .merge(df_grrr_rea_peaks, how="outer")
    .merge(df_grrr_ref_peaks_any_lt, how="outer")
)
```

```python
df_compare
```

```python
for thresh, ind in [(grrr_thresh, "streamflow"), (gf_thresh, "dis24")]:
    for af in ["a", "f"]:
        df_compare[f"{ind}_{af}_rel"] = df_compare[f"{ind}_{af}"] / thresh
```

```python
df_compare["max_rel"] = df_compare[
    [x for x in df_compare.columns if "rel" in x]
].max(axis=1)
```

```python
df_compare
```

```python
df_compare.sort_values("dis24_a", ascending=False)
```

## Plot

```python
fs_5yr = df_compare["SFED"].quantile(1 - 1 / 5)
fs_3yr = df_compare["SFED"].quantile(1 - 1 / 3)
```

```python
fs_color = "crimson"
gf_color = "royalblue"
grrr_color = "darkgreen"
```

```python
def plot_yearly_maxima(x_col, y_col, **kwargs):
    x_thresh = kwargs["x_thresh"]
    x_color = kwargs["x_color"]
    y_thresh = kwargs["y_thresh"]
    y_color = kwargs["y_color"]

    df_plot = df_compare[["year", x_col, y_col]].copy().set_index("year")
    df_plot = df_plot.dropna()

    print(df_plot.index.min(), df_plot.index.max())

    xmax = df_plot[x_col].max() * 1.1
    ymax = df_plot[y_col].max() * 1.1

    fig, ax = plt.subplots(dpi=200, figsize=(7, 7))

    ax.axvline(x_thresh, color=x_color)
    ax.axvspan(x_thresh, xmax, facecolor=x_color, alpha=0.1)

    ax.axhline(y_thresh, color=y_color)
    ax.axhspan(y_thresh, ymax, facecolor=y_color, alpha=0.1)

    for year, row in df_plot.iterrows():
        fontweight = "bold"
        if row[x_col] > x_thresh and row[y_col] > y_thresh:
            color = "k"
        elif row[x_col] > x_thresh:
            color = x_color
        elif row[y_col] > y_thresh:
            color = y_color
        else:
            fontweight = "normal"
            color = "grey"
        ax.annotate(
            year,
            (row[x_col], row[y_col]),
            color=color,
            fontweight=fontweight,
            va="center",
            ha="center",
        )

    ax.annotate(
        kwargs["y_thresh_text"],
        (0, y_thresh),
        color=y_color,
        fontstyle="italic",
        xytext=(2, 2),
        textcoords="offset points",
    )
    ax.annotate(
        kwargs["x_thresh_text"],
        (x_thresh, 0),
        rotation=90,
        ha="right",
        va="bottom",
        color=x_color,
        fontstyle="italic",
        xytext=(0, 3),
        textcoords="offset points",
    )

    ax.set_xlim((0, xmax))
    ax.set_ylim((0, ymax))
    ax.spines.top.set_visible(False)
    ax.spines.right.set_visible(False)
    return fig, ax
```

### Reanalysis

```python
fig, ax = plot_yearly_maxima(
    "dis24_a",
    "SFED",
    x_thresh=gf_thresh,
    x_color=gf_color,
    x_thresh_text=f"Threshold: {gf_thresh:,} m$^3$/s",
    y_thresh=fs_5yr,
    y_color=fs_color,
    y_thresh_text="5-yr RP",
)

ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.set_xlabel("River discharge yearly max. [GloFAS reanalysis] (m$^3$/s)")
ax.set_ylabel("Frac. area flooded yearly max. [Floodscan]")
ax.set_title("Floodscan vs. GloFAS reanalysis (1998-2024)")
```

```python
fig, ax = plot_yearly_maxima(
    "streamflow_a",
    "SFED",
    x_thresh=grrr_thresh,
    x_color=grrr_color,
    x_thresh_text=f"Threshold: {grrr_thresh:,} m$^3$/s",
    y_thresh=fs_5yr,
    y_color=fs_color,
    y_thresh_text="5-yr RP",
)

ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.set_xlabel("River discharge yearly max. [Google reanalysis] (m$^3$/s)")
ax.set_ylabel("Frac. area flooded yearly max. [Floodscan]")
ax.set_title("Floodscan vs. Google reanalysis (1998-2023)")
```

```python
fig, ax = plot_yearly_maxima(
    "streamflow_a",
    "dis24_a",
    x_thresh=grrr_thresh,
    x_color=grrr_color,
    x_thresh_text=f"Thresh.: {grrr_thresh:,} m$^3$/s",
    y_thresh=gf_thresh,
    y_color=gf_color,
    y_thresh_text=f"Thresh.: {gf_thresh:,} m$^3$/s",
)

ax.set_xlabel("River discharge yearly max. [Google reanalysis] (m$^3$/s)")
ax.set_ylabel("River discharge yearly max. [GloFAS reanalysis] (m$^3$/s)")
ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.set_title("Google vs. GloFAS reanalysis (1980-2023)")
```

### Reforecast

```python
fig, ax = plot_yearly_maxima(
    "dis24_f",
    "SFED",
    x_thresh=gf_thresh,
    x_color=gf_color,
    x_thresh_text=f"Threshold: {gf_thresh:,} m$^3$/s",
    y_thresh=fs_5yr,
    y_color=fs_color,
    y_thresh_text="5-yr RP",
)

ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.set_xlabel("River discharge yearly max. [GloFAS reforecast] (m$^3$/s)")
ax.set_ylabel("Frac. area flooded yearly max. [Floodscan]")
ax.set_title("Floodscan vs. GloFAS forecast (2003-2022)")
```

```python
fig, ax = plot_yearly_maxima(
    "streamflow_f",
    "SFED",
    x_thresh=grrr_thresh,
    x_color=grrr_color,
    x_thresh_text=f"Threshold: {grrr_thresh:,} m$^3$/s",
    y_thresh=fs_5yr,
    y_color=fs_color,
    y_thresh_text="5-yr RP",
)

ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.set_xlabel("River discharge yearly max. [Google reforecast] (m$^3$/s)")
ax.set_ylabel("Frac. area flooded yearly max. [Floodscan]")
ax.set_title("Floodscan vs. Google forecast (2016-2022)")
```

### Either

```python
fig, ax = plot_yearly_maxima(
    "max_rel",
    "SFED",
    x_thresh=1,
    x_color="teal",
    x_thresh_text="Threshold",
    y_thresh=fs_5yr,
    y_color=fs_color,
    y_thresh_text="5-yr RP",
)

ax.set_xlabel(
    "Maximum of Google or GloFAS reanalysis or reforecast,\n"
    "relative to threshold"
)
ax.set_ylabel("Frac. area flooded yearly max. [Floodscan]")
ax.set_title("Floodscan vs. combined flood forecast")
```

```python
df_daily =
```
