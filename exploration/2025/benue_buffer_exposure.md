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

# Benue buffer exposure

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
from rioxarray.exceptions import NoDataInBounds

from src.datasources import codab, hydrosheds, worldpop
from src.utils.raster import compute_density_from_grid
from src.constants import *
```

```python
gdf_benue = hydrosheds.load_benue_aoi()
```

```python
adm2 = codab.load_codab_from_blob(admin_level=2, aoi_only=True)
adm2 = adm2[adm2["ADM1_PCODE"] == ADAMAWA]
```

```python
da_wp = worldpop.load_raw_worldpop()
da_wp.attrs["_FillValue"] = np.nan
```

```python
da_wp_adm = da_wp.rio.clip(adm2.geometry)
```

```python
dicts = []
for buffer_km in [1, 5, 10, 20]:
    gdf_benue_buffer = (
        gdf_benue.to_crs(3857).buffer(buffer_km * 1000).to_crs(4326)
    )
    da_buffer = da_wp.rio.clip(gdf_benue_buffer.geometry)
    for pcode, row in adm2.set_index("ADM2_PCODE").iterrows():
        try:
            da_clip = da_buffer.rio.clip([row.geometry])
        except NoDataInBounds:
            continue
        pop = int(da_clip.sum())
        if pop < 1:
            continue
        dicts.append(
            {
                "ADM2_PCODE": pcode,
                "ADM2_EN": row["ADM2_EN"],
                "buffer_km": buffer_km,
                "pop": pop,
            }
        )

df_exp = pd.DataFrame(dicts)

df_exp = df_exp.pivot(
    index=["ADM2_PCODE", "ADM2_EN"], columns="buffer_km", values="pop"
)

df_exp.columns = [f"pop_{int(x)}km_buffer" for x in df_exp.columns]

df_exp = df_exp.reset_index()
df_exp = df_exp.fillna(0)
df_exp = df_exp.astype(int, errors="ignore")
```

```python
df_exp.sum(numeric_only=True)
```

```python
df_exp
```

```python
blob_name = f"{PROJECT_PREFIX}/processed/adamawa_benue_buffer_pop.csv"
stratus.upload_csv_to_blob(df_exp, blob_name)
```

```python
da_density = compute_density_from_grid(da_wp_adm, lat_name="y", lon_name="x")
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(10, 3))
mappable = da_density.plot(
    cmap="hot_r", ax=ax, add_colorbar=False, vmin=0, vmax=2000
)

cbar = fig.colorbar(mappable, ax=ax, fraction=0.02, pad=-0.1, extend="max")
cbar.ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f"{int(x):,}")
)
cbar.set_label("Population density (people / km$^2$)")
adm2.boundary.plot(linewidth=0.2, color="k", ax=ax)
gdf_benue.plot(linewidth=1, color="dodgerblue", ax=ax)
adm2_union = adm2.union_all()
buffer_kms = [1, 5, 10, 20]
for buffer_km in buffer_kms:
    # Create merged buffer around Benue
    buffer_geom = gdf_benue.to_crs(3857).buffer(buffer_km * 1000).union_all()
    buffer_geom = (
        gpd.GeoSeries([buffer_geom], crs=3857).to_crs(4326).union_all()
    )

    # Get the boundary line
    buffer_boundary = buffer_geom.boundary

    # Clip the boundary to only the part that intersects ADM2
    clipped_boundary = buffer_boundary.intersection(adm2_union)

    # Plot (only if not empty)
    if not clipped_boundary.is_empty:
        gpd.GeoSeries([clipped_boundary], crs=4326).plot(
            ax=ax, linewidth=0.5, linestyle="--", alpha=0.5
        )

ax.axis("off")
ax.set_ylim(top=9.8, bottom=9)
ax.set_title(
    "Adamawa population distribution\n"
    "with Benue river buffers at "
    f"{', '.join([str(x) for x in buffer_kms])} km"
)
```

```python
float(da_density.mean()) * 37000
```

```python
float(da_wp_adm.sum())
```

```python

```
