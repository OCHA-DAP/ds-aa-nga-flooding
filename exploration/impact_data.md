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

# Impact data

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import impact, codab, floodscan, worldpop
from src.constants import AOI_ADM2_PCODES, AOI_ADM1_PCODES
```

```python
adm2_pop = worldpop.load_adm2_worldpop()
```

```python
adm2 = codab.load_codab(admin_level=2)
adm2_aoi = adm2[adm2["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
adm1 = codab.load_codab(admin_level=1)
adm1_aoi = adm1[adm1["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
```

```python
nema = pd.read_excel(
    impact.NEMA_RAW_IMPACT_PATH, sheet_name="2022 NEMA Flood Data"
)
nema = nema[nema["STATE"].isin(["Borno", "Adamawa", "Yobe"])]
# all the LGA names match, fortunately
nema = nema.merge(
    adm2_aoi[["ADM2_PCODE", "ADM2_EN"]], right_on="ADM2_EN", left_on="LGA"
).drop(columns=["LGA"])
nema_sum = (
    nema.groupby(["ADM2_PCODE", "ADM2_EN"])["PERSONS AFFECTED"]
    .sum()
    .astype(int)
    .reset_index()
    .rename(columns={"PERSONS AFFECTED": "total_affected"})
)
nema_sum
```

```python
filename = (
    "2022-flood-affected-areas-2022-by-lgas-as-of-30th-october-2022-3.csv"
)
actual = pd.read_csv(impact.RAW_IMPACT_DIR / filename)
actual = actual.merge(
    adm2[["ADM2_PCODE", "ADM2_EN", "ADM1_PCODE", "ADM1_EN"]],
    right_on="ADM2_EN",
    left_on="LGA",
).drop(columns=["LGA", "State"])
actual
```

```python
fs = floodscan.load_adm2_flood_exposures()
fs = fs[(fs["year"] == 2022) & fs["ADM2_PCODE"].isin(AOI_ADM2_PCODES)].drop(
    columns="year"
)
fs = fs.merge(nema_sum, on="ADM2_PCODE", how="left")
fs["total_affected"] = fs["total_affected"].fillna(0).astype(int)
fs = fs.merge(adm2_pop, on="ADM2_PCODE")
# fs = fs.rename(columns={"Population Potentially Affected": "total_impacted"})
fs["error"] = fs["total_exposed"] - fs["total_affected"]
fs["frac_exposed"] = fs["total_exposed"] / fs["total_pop"]
fs["geo_error"] = fs["total_exposed"] / fs["total_affected"]
fs["frac_error"] = fs["error"] / fs["total_pop"]
fs["frac_affected"] = fs["total_affected"] / fs["total_pop"]
```

```python
fs
```

```python
fs_plot = adm2.merge(fs, on="ADM2_PCODE")

fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 10))

vmax = fs_plot[["frac_affected", "frac_exposed"]].max().max()

axs[0].set_title("Fraction of population affected 2022 (NEMA)")
fs_plot.plot(
    column="frac_affected",
    ax=axs[0],
    cmap="Purples",
    vmin=0,
    vmax=vmax,
)

axs[1].set_title("Fraction of population exposed 2022 (FloodScan)")
fs_plot.plot(
    column="frac_exposed",
    ax=axs[1],
    cmap="Purples",
    legend=True,
    vmin=0,
    vmax=vmax,
)


for ax in axs:
    adm1_aoi.boundary.plot(ax=ax, linewidth=0.5, color="k")
    ax.axis("off")

plt.subplots_adjust(wspace=-0.3)
```

```python
fs_plot = adm2.merge(fs, on="ADM2_PCODE")

fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 10))

vmax = fs_plot[["total_affected", "total_exposed"]].max().max()

axs[0].set_title("Total population affected 2022 (NEMA)")
fs_plot.plot(
    column="total_affected",
    ax=axs[0],
    cmap="Purples",
    legend=False,
    vmin=0,
    vmax=vmax,
)

axs[1].set_title("Total population exposed 2022 (FloodScan)")
fs_plot.plot(
    column="total_exposed",
    ax=axs[1],
    cmap="Purples",
    legend=True,
    vmin=0,
    vmax=vmax,
)

for ax in axs:
    adm1_aoi.boundary.plot(ax=ax, linewidth=0.5, color="k")
    ax.axis("off")

plt.subplots_adjust(wspace=-0.3)
```

```python

```
