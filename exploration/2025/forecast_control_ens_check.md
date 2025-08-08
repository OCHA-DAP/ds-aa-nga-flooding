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

# Forecast-reforecast check
<!-- markdownlint-disable MD013 -->
Checking control forecast vs ensemble mean for forecast and reforecast

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from src.datasources import glofas
from src.utils import cds_utils
from src.constants import *
```

## Check reforecast ensemble mean vs control

```python
df_ref_control = glofas.load_reforecast()
```

```python
df_ref_ens = pd.read_parquet(
    glofas.GF_PROC_DIR / "wuroboki_glofas_reforecast_ens.parquet"
)
```

```python
df_rea = glofas.load_glofas_reanalysis("wuroboki")
```

```python
df_rea
```

```python
df_ref_ens
```

```python
df_ref_ens_mean = (
    df_ref_ens.groupby(["time", "valid_time", "leadtime"])["dis24"]
    .mean()
    .reset_index()
)
```

```python
df_ref_ens_mean
```

```python
df_ref_control
```

```python
df_ref_compare = df_ref_control.merge(
    df_ref_ens_mean,
    on=["time", "valid_time", "leadtime"],
    suffixes=("_c", "_m"),
)
```

```python
df_ref_compare["abs_diff"] = (
    df_ref_compare["dis24_m"] - df_ref_compare["dis24_c"]
)
df_ref_compare["frac_diff"] = (
    df_ref_compare["abs_diff"] / df_ref_compare["dis24_c"]
)
df_ref_compare["abs_frac_diff"] = df_ref_compare["frac_diff"].abs()
```

```python
df_ref_compare["frac_diff"].hist(bins=50)
```

```python
df_ref_compare.groupby("leadtime")["abs_frac_diff"].mean().plot()
```

```python
df_ref_compare.corr()
```

```python
df_compare = df_ref_compare.merge(df_rea)
df_compare
```

```python
df_corr = (
    df_compare.groupby("leadtime")[["dis24", "dis24_c", "dis24_m"]]
    .corr()
    .reset_index()
)
df_corr
```

```python
df_corr[df_corr["level_1"] == "dis24"].plot(
    x="leadtime", y=["dis24_c", "dis24_m"]
)
```

## Download forecast

```python
forecast_dataset = "cems-glofas-forecast"
forecast_request = {
    "system_version": ["operational"],
    "hydrological_model": ["lisflood"],
    "product_type": ["control_forecast", "ensemble_perturbed_forecasts"],
    "variable": "river_discharge_in_the_last_24_hours",
    "year": ["2023"],
    "month": ["09"],
    "day": ["01", "02", "03", "04", "05", "06"],
    "leadtime_hour": ["24", "48", "72", "96", "120"],
    "data_format": "grib2",
    "download_format": "unarchived",
    "area": glofas.get_coords("wuroboki"),
}
```

```python
blob_name = f"{PROJECT_PREFIX}/raw/glofas/forecast/forecast_2023_test.grib"
```

```python
cds_utils.download_raw_cds_api_to_blob(
    dataset=forecast_dataset, request=forecast_request, blob_name=blob_name
)
```

```python
filepath = "temp/" + blob_name
ds_cf = xr.load_dataset(filepath, filter_by_keys={"dataType": "cf"})
ds_pf = xr.load_dataset(filepath, filter_by_keys={"dataType": "pf"})
```

```python
df_f_c_2023 = ds_cf.to_dataframe().reset_index()[
    ["time", "valid_time", "dis24"]
]
```

```python
df_f_ens_2023 = ds_pf.to_dataframe().reset_index()[
    ["time", "valid_time", "number", "dis24"]
]
```

```python
df_f_ens_mean_2023 = (
    df_f_ens_2023.groupby(["time", "valid_time"])["dis24"].mean().reset_index()
)
```

```python
df_f_compare_2023 = df_f_ens_mean_2023.merge(
    df_f_c_2023,
    on=["time", "valid_time"],
    suffixes=("_c", "_m"),
)
```

## Download reforecast

Have to download reforecast again for 2023 since previous control forecast wasn't downloaded for this year

```python
reforecast_dataset = "cems-glofas-reforecast"
reforecast_request = {
    "system_version": ["version_4_0"],
    "hydrological_model": ["lisflood"],
    "product_type": ["control_reforecast", "ensemble_perturbed_reforecast"],
    "variable": ["river_discharge_in_the_last_24_hours"],
    "hyear": ["2023"],
    "hmonth": ["09"],
    "hday": ["02", "05"],
    "leadtime_hour": ["24", "48", "72", "96", "120"],
    "data_format": "grib2",
    "download_format": "unarchived",
    "area": glofas.get_coords("wuroboki"),
}
```

```python
blob_name = f"{PROJECT_PREFIX}/raw/glofas/reforecast/reforecast_2023_test.grib"
```

```python
cds_utils.download_raw_cds_api_to_blob(
    dataset=reforecast_dataset, request=reforecast_request, blob_name=blob_name
)
```

```python
filepath = "temp/" + blob_name
ds_ref_cf = xr.load_dataset(filepath, filter_by_keys={"dataType": "cf"})
ds_ref_pf = xr.load_dataset(filepath, filter_by_keys={"dataType": "pf"})
```

```python
df_ref_c_2023 = ds_ref_cf.to_dataframe().reset_index()[
    ["time", "valid_time", "dis24"]
]
```

```python
df_ref_ens_2023 = ds_ref_pf.to_dataframe().reset_index()[
    ["time", "valid_time", "number", "dis24"]
]
```

```python
df_ref_ens_mean_2023 = (
    df_ref_ens_2023.groupby(["time", "valid_time"])["dis24"]
    .mean()
    .reset_index()
)
```

```python
df_ref_ens_mean_2023
```

```python
df_ref_compare_2023 = df_ref_c_2023.merge(
    df_ref_ens_mean_2023, on=["time", "valid_time"], suffixes=("_c", "_m")
)
```

```python
df_ref_compare_2023
```

## Compare forecast and reforecast

```python
df_compare_2023 = df_ref_compare_2023.merge(
    df_f_compare_2023, on=["time", "valid_time"], suffixes=("_ref", "_f")
)
```

```python
df_compare_2023
```

```python
for t, group in df_compare_2023.groupby("time"):
    fig, ax = plt.subplots(figsize=(8, 4), dpi=150)

    ax.plot(
        group["valid_time"],
        group["dis24_c_ref"],
        color="orange",
        linestyle="-",
        label="Ref. control",
    )
    ax.plot(
        group["valid_time"],
        group["dis24_m_ref"],
        color="orange",
        linestyle="--",
        label="Ref. ens. mean",
    )
    ax.plot(
        group["valid_time"],
        group["dis24_c_f"],
        color="red",
        linestyle="-",
        label="Forecast control",
    )
    ax.plot(
        group["valid_time"],
        group["dis24_m_f"],
        color="red",
        linestyle="--",
        label="Forecast ens. mean",
    )

    ax.set_title(f"issued time = {t}")
    ax.set_xlabel("valid_time")
    ax.set_ylabel("dis24")
    ax.legend()

    plt.tight_layout()
    plt.show()
```

```python

```
