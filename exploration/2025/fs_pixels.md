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

# Floodscan pixels

<!-- markdownlint-disable MD013 -->

Grab Floodscan pixels along Benue in Adamawa.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
from tqdm.auto import tqdm
from dask.diagnostics import ProgressBar

from src.datasources import floodscan, hydrosheds
from src.utils import blob
```

## Load data

### Benue river

```python
benue = hydrosheds.load_benue_aoi()
```

```python
benue.to_crs(3857).plot()
```

```python
benue_buffer = benue.to_crs(3857).buffer(10 * 1000).to_crs(4326)
```

```python
benue_buffer.plot()
```

### Floodscan

#### Old `.nc` file

```python
fs_old = floodscan.load_raw_nga_floodscan()
```

```python
fs_old = fs_old.rio.write_crs(4326)
```

```python
fs_old_clip = fs_old.rio.clip(benue_buffer.geometry)
```

```python
df_fs_old = fs_old_benue.to_dataframe()["SFED_AREA"].reset_index().dropna()
df_fs_old
```

```python
df_fs_old.nunique()
```

#### New COGs

```python
blob_name_format = (
    "floodscan/daily/v5/processed/aer_area_300s_v{date_str}_v05r01.tif"
)
```

```python
dates = pd.date_range("2024-01-01", "2024-12-31")
```

```python
blob_names = [blob_name_format.format(date_str=x.date()) for x in dates]
```

```python
das = []
for date in tqdm(dates):
    blob_name = blob_name_format.format(date_str=date.date())
    da_in = stratus.open_blob_cog(
        blob_name, stage="prod", container_name="raster"
    )
    da_in = da_in.sel(band=1).drop_vars("band")
    da_in["date"] = date
    das.append(da_in)
```

```python
fs_new = xr.concat(das, dim="date")
```

```python
fs_new
```

```python
fs_new_clip = fs_new.rio.clip(benue_buffer.geometry)
```

```python
fig, ax = plt.subplots()
fs_old_clip.sel(time="2022-08-24").plot(ax=ax)
benue.plot(ax=ax)
```

```python
fig, ax = plt.subplots()
fs_new_clip.sel(date="2024-02-01").plot()
benue.plot(ax=ax)
```

### Merge old and new Floodscan

```python
with ProgressBar():
    df_fs_new_raw = fs_new_clip.to_dataframe(name="SFED")
```

```python
df_fs_new = df_fs_new_raw["SFED"].reset_index().dropna()
```

```python
df_fs_new
```

Check if same pixels in each

```python
df_fs_old.nunique()
```

```python
df_fs_new.nunique()
```

```python
df_fs_new["x"].round(4)
```

Combine `df`s, and found pixel coords so they line up

```python
df_fs_combined = pd.concat(
    [
        df_fs_old.rename(
            columns={
                "time": "date",
                "lat": "y",
                "lon": "x",
                "SFED_AREA": "SFED",
            }
        ),
        df_fs_new,
    ]
)
df_fs_combined["x"] = df_fs_combined["x"].round(4)
df_fs_combined["y"] = df_fs_combined["y"].round(4)
```

```python
df_fs_combined.groupby(["x", "y"]).size()
```

Drop pixel that is missing

```python
df_fs_combined = df_fs_combined[df_fs_combined["x"] != 12.9583]
```

Double-check all pixels are there for all dates

```python
df_fs_combined.groupby(["x", "y"]).size()
```

```python
df_fs_combined.set_index(["date", "y", "x"]).to_xarray().sel(
    date="2022-08-24"
)["SFED"].plot()
```

```python
df_fs_combined.groupby(["y", "x"])["SFED"].mean().to_xarray().plot()
```

```python
df_fs_combined.pivot(columns=["x", "y"], index="date").corr()
```

```python
df_fs_combined.dtypes
```

## Write to blob

```python
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/processed/floodscan/fs_benue_pixels_1998_2024.parquet"
```

```python
blob_name
```

```python
stratus.upload_parquet_to_blob(df_fs_combined, blob_name)
```
