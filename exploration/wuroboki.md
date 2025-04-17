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

# Wuroboki - NiHSA

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
```

```python
df_nh = nihsa.load_wuroboki()
df_nh
```

```python
df_nh.sort_values("level")
```

```python
df_nh.groupby(df_nh["time"].dt.year)["level"].max().reset_index().sort_values(
    "level"
)
```

```python
df_nh["level_change"] = df_nh["level"] - df_nh["level"].shift()
```

```python
df_plot = df_nh[df_nh["time"].dt.year >= 1979].copy()
df_plot["year"] = df_plot["time"].dt.year
```

```python
n_years = df_plot["year"].nunique()
```

```python
n_years
```

```python
df_plot["level_change"].min()
```

```python
plot_col = "level"

ymax = df_plot[plot_col].max()
ymin = min(0, df_plot[plot_col].min())

ncols = 3
nrows = round(n_years / ncols) + 1

fig, axes = plt.subplots(
    nrows=nrows, ncols=ncols, figsize=(ncols * 5, nrows * 3), dpi=200
)
axes = axes.flatten()  # Flatten axes array for easy indexing

# Loop through each year and plot in corresponding subplot
for i, year in enumerate(df_plot["year"].unique()):
    ax = axes[i]
    year_data = df_plot[df_plot["year"] == year]

    # Plot the data
    ax.plot(year_data["time"], year_data[plot_col])  # Adjust 'level' as needed
    ax.set_title(f"Year {year}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Level")

    # Set the y-axis to range from 0 to the maximum value
    ax.set_ylim(ymin, ymax)

    # Set x-axis to span from Jan 1 to Dec 31 for each year
    start_date = pd.Timestamp(f"{year}-01-01")
    end_date = pd.Timestamp(f"{year}-12-31")
    ax.set_xlim(start_date, end_date)

    # Format x-axis to show months/days (e.g., Jan 01, Feb 01, etc.)
    ax.xaxis.set_major_locator(
        mdates.MonthLocator()
    )  # Show major ticks for each month
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%b")
    )  # Month and day format

# Adjust layout and display
plt.tight_layout()
plt.show()
```

```python
df_nh
```

```python
year = 2021
months = range(7, 11)
df_nh[
    (df_nh["time"].dt.year == year) & (df_nh["time"].dt.month.isin(months))
].plot(x="time", y="level_change")
```
