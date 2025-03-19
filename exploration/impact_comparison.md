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

# Impact comparison

Comparison of historical impact from various sources
<!-- markdownlint-disable MD013 -->

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import codab, floodscan, hydrosheds
from src.utils import blob
from src.constants import *
```

## Load data

### CODAB

```python
adm1 = codab.load_codab_from_blob(admin_level=1, aoi_only=True)
```

```python
adm2 = codab.load_codab_from_blob(admin_level=2, aoi_only=True)
```

```python
adm2
```

```python
adm2.plot()
```

### NiHSA flood record

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/nihsa/floodhistory_2013_2023.parquet"
)
df_nihsa_record = blob.load_parquet_from_blob(blob_name)
```

```python
df_nihsa_record[
    df_nihsa_record.duplicated(subset=["ADM2_PCODE", "Year"], keep=False)
]
```

```python
df_nihsa_record_sum = (
    df_nihsa_record.groupby("ADM2_PCODE")["Flooded"].sum().reset_index()
)
```

```python
df_nihsa_record_sum.hist("Flooded")
```

```python
fig, ax = plt.subplots(dpi=200)

col = "Flooded"

gdf_plot = adm2.merge(df_nihsa_record_sum)
gdf_plot = gdf_plot[gdf_plot[col] >= 1]
gdf_plot[col] = gdf_plot[col].astype("category")
gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="Spectral_r",
    alpha=0.7,
    legend_kwds={
        "bbox_to_anchor": (1, 1),
        "loc": "upper left",
        "title": "# years",
    },
)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title("Count of years flooded (2013-2023) [NiHSA]")
```

```python
df_nihsa_record_sum.merge(
    adm2[["ADM2_PCODE", "ADM2_EN", "ADM1_EN"]]
).sort_values(["Flooded", "ADM1_EN", "ADM2_EN"], ascending=False).iloc[
    :20
].drop(
    columns="ADM2_PCODE"
).rename(
    columns={"ADM2_EN": "LGA", "ADM1_EN": "State", "Flooded": "# years"}
)[
    ["State", "LGA", "# years"]
]
```

### NEMA flood risk

```python
df_nema_risk
```

```python
df_nema_risk["riv_num"] = df_nema_risk["Riverine Flood Risk"].replace(
    {"High": 3, "Medium": 2, "Low": 1}
)
df_nema_risk["flash_num"] = df_nema_risk["Flash Flood Risk"].replace(
    {"High": 3, "Medium": 2, "Low": 1}
)
```

```python
fig, ax = plt.subplots(dpi=200)

col = "Riverine Flood Risk"

gdf_plot = adm2.merge(df_nema_risk)
gdf_plot[col] = (
    gdf_plot[col]
    .astype("category")
    .cat.reorder_categories(["High", "Medium", "Low"])
)

gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="autumn",
    alpha=0.5,
    legend_kwds={
        "bbox_to_anchor": (1, 1),
        "loc": "upper left",
        "title": "Risk Level",
    },
)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title(f"{col} [NEMA]")
```

```python
fig, ax = plt.subplots(dpi=200)

col = "Flash Flood Risk"

gdf_plot = adm2.merge(df_nema_risk)
gdf_plot[col] = (
    gdf_plot[col]
    .astype("category")
    .cat.reorder_categories(["High", "Medium", "Low"])
)

gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    alpha=0.5,
    cmap="autumn",
    legend_kwds={
        "bbox_to_anchor": (1, 1),
        "loc": "upper left",
        "title": "Risk Level",
    },
)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title(f"{col} [NEMA]")
```

```python
df_nema_risk[df_nema_risk["riv_num"] + df_nema_risk["flash_num"] == 6][
    ["STATE", "ADM2_EN"]
].sort_values(["STATE", "ADM2_EN"]).rename(columns={"ADM2_EN": "LGA"})
```

### Floodscan exposure

```python
fs = floodscan.load_adm2_flood_exposures()
fs_mean = fs.groupby("ADM2_PCODE")["total_exposed"].mean().reset_index()
```

```python
fig, ax = plt.subplots(dpi=200)

adm2.merge(fs_mean).plot(column="total_exposed", legend=True, ax=ax, alpha=0.7)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title("Floodscan mean historical exposure (1998-2023)")
```

```python
fs_mean
```

### NEMA UNICEF impact

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/nema/NEMA Copy of FLOOD DISASTER _DATA processed.csv"
df_unicef = blob.load_csv_from_blob(blob_name)
df_unicef = (
    df_unicef.melt(id_vars=[x for x in df_unicef.columns if "ADM" in x])
    .drop(columns="variable")
    .rename(columns={"value": "year"})
)
df_unicef = df_unicef.dropna()
df_unicef["year"] = df_unicef["year"].astype(int)
```

```python
df_unicef_count = (
    df_unicef.groupby("ADM2_PCODE")
    .size()
    .reset_index()
    .rename(columns={0: "count_floods"})
)
```

```python
df_unicef_unique_count = (
    df_unicef.groupby("ADM2_PCODE")["year"]
    .nunique()
    .reset_index()
    .rename(columns={"year": "count_years"})
)
```

```python
df_unicef_count = df_unicef_count.merge(df_unicef_unique_count)
```

```python
df_unicef_count
```

```python
fig, ax = plt.subplots(dpi=200)

col = "count_years"

gdf_plot = adm2.merge(df_unicef_count)
gdf_plot[col] = gdf_plot[col].astype("category")
gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="Spectral_r",
    alpha=0.7,
    legend_kwds={
        "bbox_to_anchor": (1, 1),
        "loc": "upper left",
        "title": "# years",
    },
)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title("Count of years flooded (2013-2023) [UNICEF]")
```

```python
fig, ax = plt.subplots(dpi=200)

col = "count_floods"

gdf_plot = adm2.merge(df_unicef_count)
gdf_plot[col] = gdf_plot[col].astype("category")
gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="Spectral_r",
    alpha=0.7,
    legend_kwds={
        "bbox_to_anchor": (1, 1),
        "loc": "upper left",
        "title": "# floods",
    },
)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title("Count of flood reports (2013-2023) [UNICEF]")
```

```python
df_unicef_count[df_unicef_count["count_floods"] >= 4].merge(
    adm2[["ADM2_PCODE", "ADM2_EN", "ADM1_EN"]]
).sort_values(["ADM1_EN", "ADM2_EN"]).sort_values(
    "count_floods", ascending=False
).rename(
    columns={"ADM2_EN": "LGA", "ADM1_EN": "State"}
)[
    ["State", "LGA", "count_floods"]
]
```

```python
df_combined = (
    adm2.merge(fs_mean)
    .merge(df_unicef_count)
    .merge(df_nema_risk)
    .merge(df_nihsa_record_sum)
)
df_combined
```

```python
df_combined.columns
```

```python
benue = hydrosheds.load_benue_aoi()
```

```python
gdf_plot = df_combined[
    (df_combined["count_floods"] >= 3)
    & ((df_combined["riv_num"] >= 2) | (df_combined["flash_num"] >= 2))
    & (df_combined["Flooded"] >= 3)
].copy()

fig, ax = plt.subplots(dpi=200)

col = "total_exposed"


gdf_plot.plot(column=col, legend=True, ax=ax, cmap="Purples", vmin=0)

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")
benue.plot(ax=ax, color="dodgerblue", linewidth=2)

ax.axis("off")
ax.set_title("Mean historical flood exposure in filtered areas")
```

```python
gdf_plot["Total Exposed"] = gdf_plot["total_exposed"].astype(int)
gdf_plot.sort_values("Total Exposed", ascending=False)[
    ["ADM1_EN", "ADM2_EN", "Total Exposed"]
].rename(columns={"ADM2_EN": "LGA", "ADM1_EN": "State"})
```

```python

```
