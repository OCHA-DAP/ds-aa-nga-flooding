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

# Floodscan

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from adjustText import adjust_text

from src.datasources import floodscan, codab, worldpop
from src.constants import *
```

```python
# floodscan.clip_nga_from_glb()
```

```python
# floodscan.calculate_exposure_raster()
```

```python
# floodscan.calculate_adm2_exposures()
```

```python
adm2 = codab.load_codab(admin_level=2)
# adm2_aoi = adm2[adm2["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
adm1 = codab.load_codab(admin_level=1)
# adm1_aoi = adm1[adm1["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
```

```python
pop = worldpop.load_raw_worldpop()
# pop_aoi = pop.rio.clip(adm2_aoi.geometry, all_touched=True)
# pop_aoi = pop_aoi.where(pop_aoi > 0)
```

```python
fs_raster = floodscan.load_raw_nga_floodscan()
fs_raster = fs_raster.rio.write_crs(4326)
fs_aoi = fs_raster.rio.clip(adm2.geometry, all_touched=True)
fs_aoi_year = fs_aoi.groupby("time.year").max()
fs_aoi_mean = fs_aoi_year.mean(dim="year")
```

```python
adm2_pop = worldpop.load_adm2_worldpop()
```

```python
adm2_pop
```

```python
exposure = floodscan.load_adm2_flood_exposures()
exposure = exposure.merge(adm2_pop, on="ADM2_PCODE")
exposure["frac_exposed"] = exposure["total_exposed"] / exposure["total_pop"]
```

```python
exposure
```

```python
avg_exposure = (
    exposure.groupby("ADM2_PCODE").mean().reset_index().drop(columns=["year"])
)
int_cols = ["total_exposed", "total_pop"]
avg_exposure[int_cols] = avg_exposure[int_cols].astype(int)
avg_exposure_plot = adm2.merge(avg_exposure, on="ADM2_PCODE")
# avg_exposure_plot_aoi = avg_exposure_plot[
#     avg_exposure_plot["ADM1_PCODE"].isin(AOI_ADM1_PCODES)
# ]
```

```python
avg_exposure_plot
```

```python
fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 10))

for j, variable in enumerate(["total_exposed", "frac_exposed"]):
    avg_exposure_plot.plot(
        column=variable, ax=axs[j], legend=True, cmap="Purples"
    )
    # for index, row in (
    #     avg_exposure_plot_aoi.sort_values(variable).iloc[-10:].iterrows()
    # ):
    #     centroid = row["geometry"].centroid

    #     axs[j].annotate(
    #         row["ADM2_EN"],
    #         xy=(centroid.x, centroid.y),
    #         xytext=(0, 0),
    #         textcoords="offset points",
    #         ha="center",
    #         va="center",
    #     )

    adm1.boundary.plot(ax=axs[j], linewidth=0.5, color="k")
    axs[j].axis("off")


axs[0].set_title("Average total population exposed to flooding each year")
axs[1].set_title(
    "Average fraction of population exposed to flooding each year"
)

plt.subplots_adjust(wspace=0)
```

```python
cols = [
    # "ADM1_PCODE",
    "ADM1_EN",
    "ADM2_PCODE",
    "ADM2_EN",
    # "total_pop",
    "total_exposed",
    "frac_exposed",
    # "geometry",
]
avg_exposure_plot[cols].sort_values("total_exposed", ascending=False).iloc[
    :10
].style.background_gradient(cmap="Purples")
```

```python
cols = [
    "ADM1_PCODE",
    "ADM1_EN",
    "ADM2_PCODE",
    "ADM2_EN",
    "total_pop",
    "total_exposed",
    "frac_exposed",
    # "geometry",
]
filename = "nga_wholecountry_adm2_average_flood_exposed.csv"
avg_exposure_plot[cols].sort_values("total_exposed", ascending=False).to_csv(
    floodscan.PROC_FS_DIR / filename, index=False
)
```

```python
exposure_raster = floodscan.load_raster_flood_exposures()
# exposure_raster_aoi = exposure_raster.rio.clip(
#     adm2_aoi.geometry, all_touched=True
# )
```

```python
exposure_raster_aoi_mean = exposure_raster_aoi.mean(dim="year")
exposure_raster_aoi_mean = exposure_raster_aoi_mean.where(
    exposure_raster_aoi_mean > 5
)
```

```python
fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(20, 20))

# pop
pop_aoi.plot(ax=axs[0], cmap="Greys", vmax=1000, add_colorbar=False)
axs[0].set_title("Population, 2020")

# flooding
fs_aoi_mean.plot(ax=axs[1], cmap="Blues", add_colorbar=False)
axs[1].set_title("Mean of maximum yearly flooded fraction, 1998-2023")

# exposure
exposure_raster_aoi_mean.plot(
    ax=axs[2], cmap="Purples", vmax=100, add_colorbar=False
)
axs[2].set_title("Average population exposued to flooding, 1998-2023")

for ax in axs:
    adm2_aoi.boundary.plot(ax=ax, linewidth=0.2, color="k")
    ax.axis("off")

plt.subplots_adjust(wspace=0.2)
```

```python
exposure_raster_df = exposure_raster.to_dataframe("pop_exposed")[
    "pop_exposed"
].reset_index()
```

```python
exposure_raster_df
```

```python

```
