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

# GloFAS reforecast

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import glofas
from src.utils.rp_calc import calculate_groups_rp, calculate_one_group_rp
```

```python
df_ref_ens = pd.read_parquet(
    glofas.GF_PROC_DIR / "wuroboki_glofas_reforecast_ens.parquet"
)
```

```python
df_ref = (
    df_ref_ens.groupby(["valid_time", "leadtime"])["dis24"]
    .mean()
    .reset_index()
)
```

```python
df_ref[
    (df_ref["valid_time"].dt.year == 2003) & (df_ref["leadtime"] == 1)
].set_index("valid_time").plot(y="dis24")
```

```python
df_ref_peaks = (
    df_ref.groupby([df_ref["valid_time"].dt.year, "leadtime"])["dis24"]
    .max()
    .reset_index()
)
```

```python
df_ref_peaks
```

```python
df_ref_peaks = calculate_groups_rp(
    df_ref_peaks, by=["leadtime"], col_name="dis24", ascending=False
)
```

```python
df_ref_peaks
```

```python
df_rea = glofas.load_glofas_reanalysis(station_name="wuroboki")
```

```python
df_rea = df_rea.rename(columns={"time": "valid_time"})
```

```python
df_rea[df_rea["valid_time"].dt.year == 2003].set_index("valid_time").plot()
```

```python
df_rea_peaks = (
    df_rea.groupby(df_rea["valid_time"].dt.year)["dis24"].max().reset_index()
)
```

```python
df_rea_peaks.plot(x="valid_time", y="dis24")
```

```python
df_rea_peaks = calculate_one_group_rp(
    df_rea_peaks, col_name="dis24", ascending=False
)
```

```python
rp_ra = 5.4
```

```python
thresh_ra = 3110
```

```python
# df_rea_peaks["trig"] = df_rea_peaks["dis24_rp"] >= rp_ra
df_rea_peaks["trig"] = df_rea_peaks["dis24"] >= thresh_ra
```

```python
df_rea_peaks
```

```python
df_compare = df_rea_peaks.merge(
    df_ref_peaks, suffixes=("_a", "_f"), on="valid_time"
)
```

```python
for lt, group in df_compare.groupby("leadtime"):
    print(lt)
    display(group.sort_values("dis24_rank_f"))
```

```python
lt_max = 5
thresh_ref = 3130
df_compare[
    (df_compare["dis24_f"] >= thresh_ref) & (df_compare["leadtime"] <= lt_max)
]
```

```python
df_ref["issued_time"] = df_ref["valid_time"] - df_ref["leadtime"].apply(
    lambda x: pd.Timedelta(days=x - 1)
)
```

```python
df_ref
```

```python
df_ref_trig = df_ref[
    (df_ref["dis24"] >= thresh_ref) & (df_ref["leadtime"] <= lt_max)
]
```

```python
df_ref["issued_time"].unique()
```

```python
df_ref_trig.sort_values(["issued_time", "valid_time"])
```

```python
df_ref_trig.loc[
    df_ref_trig.groupby(df_ref_trig["valid_time"].dt.year)[
        "issued_time"
    ].idxmin()
]
```

```python
dicts = []
for lt_max in df_compare["leadtime"].unique():
    print(lt_max)
    dff = df_compare[df_compare["leadtime"] <= lt_max]
    max_false_value = dff[~dff["trig"]]["dis24_f"].max()
    min_true_value = dff[dff["trig"] & (dff["dis24_f"] >= max_false_value)][
        "dis24_f"
    ].min()
    n_trig_years = dff[dff["dis24_f"] > max_false_value][
        "valid_time"
    ].nunique()
    display(
        dff[dff["dis24_f"] >= max_false_value].sort_values(
            "dis24_f", ascending=False
        )
    )
    dicts.append(
        {
            "lt_max": lt_max,
            "min_true": min_true_value,
            "max_false": max_false_value,
            "n_trig": n_trig_years,
        }
    )

df_threshs = pd.DataFrame(dicts)
```

```python
df_threshs["margin"] = df_threshs["min_true"] - df_threshs["max_false"]
df_threshs["mean"] = (df_threshs["min_true"] + df_threshs["max_false"]) / 2
df_threshs["mean_round"] = df_threshs["mean"].apply(round)
```

```python
df_threshs["min_true_round"] = df_threshs["min_true"].astype(int) + 1
df_threshs["max_false_round"] = df_threshs["max_false"].astype(int)
```

```python
df_threshs
```

```python
df_compare[df_compare["trig"]]["valid_time"].unique()
```

```python
df_compare["rel_f_a"] = df_compare["dis24_f"] / df_compare["dis24_a"]
```

```python
fig, ax = plt.subplots()
df_compare.groupby("leadtime")["rel_f_a"].mean().plot(ax=ax)
ax.set_ylabel("Forecast relative to reanalysis,\nmean of yearly peaks")
```

```python
df_compare_daily = df_ref.merge(df_rea, on="valid_time", suffixes=("_f", "_a"))
df_compare_daily
```

```python
df_compare_daily["rel_f_a"] = (
    df_compare_daily["dis24_f"] / df_compare_daily["dis24_a"]
)
```

```python
fig, ax = plt.subplots()
df_compare_daily.groupby("leadtime")["rel_f_a"].mean().plot(ax=ax)
ax.set_ylabel("Forecast relative to reanalysis,\nmean of all daily values")
```
