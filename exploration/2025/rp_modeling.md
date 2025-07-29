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

# RP modeling

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import grrr, glofas
from src.utils import rp_calc
```

```python
gf_thresh = 3130
```

```python
df_ref_ens = pd.read_parquet(
    glofas.GF_PROC_DIR / "wuroboki_glofas_reforecast_ens.parquet"
)
df_gf_ref = (
    df_ref_ens.groupby(["valid_time", "leadtime"])["dis24"]
    .mean()
    .reset_index()
)
```

```python
df_gf_rea = glofas.load_glofas_reanalysis(station_name="wuroboki")
```

```python
ds_rf = grrr.load_reforecast()
df_grrr_rf = grrr.process_reforecast(ds_rf)
```

```python
ds_ra = grrr.load_reanalysis()
df_grrr_ra = grrr.process_reanalysis(ds_ra)
```

```python
ds_return_periods = grrr.load_return_periods()
```

```python
return_periods_dict = {}

for var_name in ds_return_periods.data_vars:
    return_periods_dict[int(var_name.split("_")[-1])] = ds_return_periods[
        var_name
    ].item()

df_rp = pd.DataFrame([return_periods_dict])
df_rp = (
    df_rp.T.reset_index()
    .rename(columns={"index": "return_period", 0: "streamflow"})
    .sort_values("return_period", ascending=True)
)
```

```python
df_rp
```

```python
df_gf_rea_peaks = (
    df_gf_rea.groupby(df_gf_rea["time"].dt.year)["dis24"].max().reset_index()
)
```

```python
df_gf_rea_peaks = rp_calc.calculate_one_group_rp(
    df_gf_rea_peaks, "dis24", ascending=False
)
```

```python
df_gf_rea_peaks.sort_values("dis24_rank")
```

```python
df_gf_rea_modeled_rp = rp_calc.estimate_return_periods_from_values(
    df_gf_rea,
    date_col="time",
    val_col="dis24",
    values=df_gf_rea_peaks["dis24"],
)
```

```python
gf_thresh_mod_rp = rp_calc.estimate_return_periods_from_values(
    df_gf_rea,
    date_col="time",
    val_col="dis24",
    values=[gf_thresh],
).iloc[0]["return_period"]
```

```python
gf_thresh_mod_rp
```

```python
df_gf_rea_modeled_rp
```

```python
df_gf_rea_peaks = df_gf_rea_peaks.merge(
    df_gf_rea_modeled_rp.rename(
        columns={"value": "dis24", "return_period": "mod_rp"}
    )
)
```

```python
df_gf_rea_peaks.sort_values("dis24_rank")
```

```python
fig, ax = plt.subplots()

df_gf_rea_peaks.sort_values("dis24").plot(
    x="mod_rp", y="dis24", ax=ax, label="model"
)
df_gf_rea_peaks.sort_values("dis24").plot(
    x="dis24_rp", y="dis24", ax=ax, label="empirical"
)
ax.set_xlim((1, 20))
ax.set_ylim(top=6000)
```

```python
df_grrr_rf_peaks = (
    df_grrr_rf.groupby(df_grrr_rf["valid_time"].dt.year)["streamflow"]
    .max()
    .reset_index()
)
```

```python
df_grrr_rf_peaks
```

```python
df_grrr_ra_peaks = (
    df_grrr_ra.groupby(df_grrr_ra["valid_time"].dt.year)["streamflow"]
    .max()
    .reset_index()
)
```

```python
df_grrr_ra_peaks = rp_calc.calculate_one_group_rp(
    df_grrr_ra_peaks, col_name="streamflow", ascending=False
)
```

```python
df_grrr_ra_peaks.sort_values("valid_time")
```

```python
grrr_trig_years = (
    df_grrr_ra_peaks[df_grrr_ra_peaks["valid_time"] >= 1998]
    .sort_values("streamflow_rank")
    .iloc[:5]["valid_time"]
    .to_list()
)
```

```python
min_true_rf = df_grrr_rf_peaks[
    df_grrr_rf_peaks["valid_time"].isin(grrr_trig_years)
]["streamflow"].min()
```

```python
max_false_rf = df_grrr_rf_peaks[
    ~df_grrr_rf_peaks["valid_time"].isin(grrr_trig_years)
]["streamflow"].max()
```

```python
min_true_ra = df_grrr_ra_peaks[
    (df_grrr_ra_peaks["valid_time"] >= 1998)
    & (df_grrr_ra_peaks["valid_time"].isin(grrr_trig_years))
]["streamflow"].min()
```

```python
max_false_ra = df_grrr_ra_peaks[
    (df_grrr_ra_peaks["valid_time"] >= 1998)
    & (~df_grrr_ra_peaks["valid_time"].isin(grrr_trig_years))
]["streamflow"].max()
```

```python
print(min_true_rf, min_true_ra)
min_true = min(min_true_rf, min_true_ra)
```

```python
print(max_false_rf, max_false_ra)
max_false = max(max_false_rf, max_false_ra)
```

```python
min_true_rf, min_true_ra
```

```python
df_gf_ref_peaks = (
    df_gf_ref.groupby([df_gf_ref["valid_time"].dt.year, "leadtime"])["dis24"]
    .max()
    .reset_index()
)
```

```python
df_gf_rea_peaks
```

```python
gf_trig_years = (
    df_gf_rea_peaks[
        (df_gf_rea_peaks["time"] >= 1998) & (df_gf_rea_peaks["time"] <= 2023)
    ]
    .sort_values("dis24_rank")
    .iloc[:5]["time"]
    .to_list()
)
```

```python
gf_trig_years
```

```python
df_gf_ref_peaks[
    (df_gf_ref_peaks["valid_time"].isin(gf_trig_years))
    & (df_gf_ref_peaks["leadtime"] <= 5)
]["dis24"].min()
```

```python

```

```python
min_true, max_false
```

```python
round((max_false + min_true) / 2)
```

```python
df_grrr_ra_peaks
```

```python
df_grrr_ra
```

```python
gf_thresh_mod_rps = rp_calc.estimate_return_periods_from_values(
    df_gf_rea,
    date_col="time",
    val_col="dis24",
    values=[3121, 3143],
)["return_period"]

rp_calc.estimate_return_periods(
    df_grrr_ra,
    date_col="valid_time",
    val_col="streamflow",
    target_rps=gf_thresh_mod_rps,
)
```

```python
rp_calc.estimate_return_periods_from_values(
    df_gf_rea,
    date_col="time",
    val_col="dis24",
    values=[3121, 3143],
)
```

```python
rp_calc.estimate_return_periods(
    df_grrr_ra,
    date_col="valid_time",
    val_col="streamflow",
)
```

```python
df_grrr_rf["leadtime"]
```

```python
df_grrr_ra.merge(df_grrr_rf[df_grrr_rf["leadtime"] == 1], on="valid_time")
```
