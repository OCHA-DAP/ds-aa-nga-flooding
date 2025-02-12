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

# GloFAS new reanalysis

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import xarray as xr

from src.datasources import nihsa, glofas
from src.utils import blob
```

```python
station_name = "wuroboki"
```

```python
glofas.download_glofas_reanalysis_to_blob(station_name=station_name)
```

```python
glofas.process_glofas_reanalysis(station_name=station_name)
```

```python
df_ra = glofas.load_glofas_reanalysis(station_name=station_name)
```

```python
df_ra
```

```python
df_nh = nihsa.load_wuroboki()
```

```python
df_nh
```

```python
df_compare = df_ra.merge(df_nh, how="left")
```

```python
df_compare
```

```python
months = range(7, 11)
df_plot = df_compare[
    (df_compare["time"].dt.month.isin(months))
    # & (df_compare["time"].dt.year.isin(nihsa.WUROBOKI_COMPLETE_YEARS))
]

dis_max = df_plot["dis24"].max()
level_max = df_plot["level"].max()

n_years = df_plot["time"].dt.year.nunique()

ncols = 3
nrows = round(n_years / ncols) + 1

fig, axes = plt.subplots(
    nrows=nrows, ncols=ncols, figsize=(ncols * 5, nrows * 3)
)
axes = axes.flatten()

for j, year in enumerate(df_plot["time"].dt.year.unique()):
    dff = df_plot[df_plot["time"].dt.year == year]
    ax = axes[j]
    ax2 = ax.twinx()

    ax.plot(
        dff["time"],
        dff["level"],
        color="dodgerblue",
        label="NiHSA\n(mm, left axis)",
    )
    ax.set_ylim(bottom=0, top=level_max * 1.1)

    ax2.plot(
        dff["time"],
        dff["dis24"],
        color="darkorange",
        label="GloFAS\n(m$^{3}$/s, right axis)",
    )
    ax2.set_ylim(bottom=0, top=dis_max * 1.1)

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.set_title(year)

    if j == 0:
        ax.legend(loc="upper left")
        ax2.legend(loc="upper right")


fig.suptitle(
    f"Comparison of NiHSA and GloFAS reanalysis for {station_name.capitalize()}",
    fontsize=16,
    y=1,
)

plt.tight_layout()

plt.savefig(
    "temp/wuroboki_nihsa_glofas_comparison.png", dpi=200, bbox_inches="tight"
)
```

```python
df_compare.corr()
```

```python
df_peaks = df_compare.groupby(df_compare["time"].dt.year.rename("year")).agg(
    dis24_max=("dis24", "max"),
    dis24_max_date=("dis24", lambda x: df_compare.loc[x.idxmax(), "time"]),
    level_max=("level", "max"),
    level_max_date=(
        "level",
        lambda x: (
            df_compare.loc[x.idxmax(), "time"] if x.notna().any() else pd.NaT
        ),
    ),
)
```

```python
df_peaks.corr()
```

```python
df_peaks.loc[nihsa.WUROBOKI_COMPLETE_YEARS].corr()
```

```python
df_plot["dis24_max"].quantile(1 - 1 / rp)
```

```python
df_peaks.loc[2000:]
```

```python

```

```python
# df_plot = df_peaks.loc[nihsa.WUROBOKI_COMPLETE_YEARS].copy()
df_plot = df_peaks.loc[2013:]
# df_plot = df_peaks.copy()

rp = 3

fig, ax = plt.subplots(dpi=200, figsize=(7, 7))

dis24_thresh = df_plot["dis24_max"].quantile(1 - 1 / rp)
level_thresh = df_plot["level_max"].quantile(1 - 1 / rp)

for year, row in df_plot.iterrows():
    ax.annotate(
        year,
        (row["dis24_max"], row["level_max"]),
        ha="center",
        va="center",
        fontsize=8,
    )

ax.axvline(dis24_thresh)
ax.axhline(level_thresh)

ax.set_ylim(bottom=0, top=df_plot["level_max"].max() * 1.1)
ax.set_xlim(left=0, right=df_plot["dis24_max"].max() * 1.1)

plt.tight_layout()
```
