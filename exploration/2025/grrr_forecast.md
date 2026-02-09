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

<!-- #region editable=true slideshow={"slide_type": ""} -->
# Google forecast
<!-- markdownlint-disable MD013 -->
Check to see how many leadtimes of forecast we can include using the same threshold as for the reanalysis, without triggering for any different years.
<!-- #endregion -->

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus

from src.datasources import grrr
from src.utils.rp_calc import calculate_groups_rp, calculate_one_group_rp
```

## Load and process

```python
ds_ra = grrr.load_reanalysis()
df_ra = grrr.process_reanalysis(ds_ra)
```

```python
df_ra["valid_year"] = df_ra["valid_time"].dt.year
```

```python
df_ra_recent = df_ra[df_ra["valid_year"] >= 1998]
```

```python
df_ra_peaks = (
    df_ra_recent.groupby("valid_year")["streamflow"].max().reset_index()
)
```

```python
rp_ra = 5.4
```

```python
df_ra_peaks = calculate_one_group_rp(
    df_ra_peaks, col_name="streamflow", ascending=False
)
```

```python
df_ra_peaks["trig"] = df_ra_peaks["streamflow_rp"] >= rp_ra
```

```python
df_ra_peaks["trig"].sum()
```

```python
ds_rf = grrr.load_reforecast()
df_rf = grrr.process_reforecast(ds_rf)
```

```python
df_rf["valid_year"] = df_rf["valid_time"].dt.year
```

```python
df_rf_peaks = (
    df_rf.groupby(["valid_year", "leadtime"])["streamflow"].max().reset_index()
)
```

## Look at yearly peaks

```python
df_rf_peaks = df_rf_peaks[df_rf_peaks["valid_year"] < 2023]
```

```python
df_rf_peaks = calculate_groups_rp(
    df_rf_peaks, ["leadtime"], col_name="streamflow", ascending=False
)
```

We can iterate over the leadtimes to see if the ranking of yearly peaks changes at all.

```python
for lt, group in df_rf_peaks.groupby("leadtime"):
    print(lt)
    display(group.sort_values("streamflow_rank"))
```

Looks like it doesn't, nice. But we can just double-check against the reanalysis peaks.

```python
df_compare = df_rf_peaks.merge(
    df_ra_peaks, on="valid_year", suffixes=("_f", "_a")
)
```

```python
for lt, group in df_compare.groupby("leadtime"):
    print(lt)
    display(group.sort_values("streamflow_rank_a"))
```

Looks like we're good. Then we can check what the maximum non-trigger-year peak is. The threshold just needs to be above this.

```python
df_compare[~df_compare["trig"]]["streamflow_f"].max()
```

```python
thresh_ra = 1212
```

Looks like the old value is fine, so we can just triple-check that only 2019 and 2022 would've triggered with this value for the reforecast.

```python
df_rf["trig"] = df_rf["streamflow"] >= thresh_ra
```

```python
df_rf[df_rf["trig"]]["valid_year"].unique()
```

```python

```
