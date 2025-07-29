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
<!-- markdownlint-disable MD013 -->
Comparison of historical impact from various sources, each of which have their own notebook for basic processing:

- NEMA flood risk
- NiHSA flood record
- UNICEF impact
- IOM impact

Also includes Floodscan exposure.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

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
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/processed/nihsa/floodhistory_2013_2023.parquet"
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
gdf_plot[col] = (
    gdf_plot[col]
    .astype("category")
    .cat.add_categories([0])
    .cat.reorder_categories(range(6))
)
gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="hot_r",
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
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/raw/AA-nigeria_data/NEMA/Flood Risk Excel Data 2.xlsx"
df_nema_risk = blob.load_excel_from_blob(blob_name)
```

```python
df_nema_risk
```

```python
df_nema_risk["riv_num"] = df_nema_risk["Riverine Flood Risk"].replace(
    {"Very High": 4, "High": 3, "Medium": 2, "Low": 1}
)
df_nema_risk["flash_num"] = df_nema_risk["Flash Flood Risk"].replace(
    {"Very High": 4, "High": 3, "Medium": 2, "Low": 1}
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
    .cat.add_categories(["None"])
)

gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="hot",
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
    .cat.add_categories(["None"])
)

gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    alpha=0.5,
    cmap="hot",
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
df_nema_risk["riv_num"].unique()
```

```python
df_nema_risk["flash_num"].unique()
```

```python
df_nema_risk[
    (
        df_nema_risk["riv_num"].astype(float)
        + df_nema_risk["flash_num"].astype(float)
        == 6
    )
    & (df_nema_risk["ADM1_PCODE"].isin(AOI_ADM1_PCODES))
][["STATE", "ADM2_EN"]].sort_values(["STATE", "ADM2_EN"]).rename(
    columns={"ADM2_EN": "LGA"}
)
```

### Floodscan exposure

```python
fs = floodscan.load_adm2_flood_exposures()
fs_mean = fs.groupby("ADM2_PCODE")["total_exposed"].mean().reset_index()
```

```python
fig, ax = plt.subplots(dpi=200)

adm2.merge(fs_mean).plot(
    column="total_exposed", legend=True, ax=ax, alpha=0.7, cmap="hot_r"
)
cbar = ax.get_figure().axes[-1]  # Get last axis (colorbar)
cbar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title("Floodscan mean historical exposure (1998-2023)")
```

```python
fs_mean.merge(adm2[["ADM2_PCODE", "ADM2_EN", "ADM1_EN"]]).rename(
    columns={
        "ADM2_EN": "LGA",
        "ADM1_EN": "State",
        "total_exposed": "Total Exposed",
    }
)[["State", "LGA", "Total Exposed"]].sort_values(
    "Total Exposed", ascending=False
).astype(
    int, errors="ignore"
).iloc[
    :20
].style.background_gradient(
    subset=["Total Exposed"], cmap="hot_r", vmin=0
).format(
    {"Total Exposed": "{:,}"}
)
```

```python

```

### NEMA UNICEF impact

```python
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/processed/nema/NEMA Copy of FLOOD DISASTER _DATA processed.csv"
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
gdf_plot[col] = (
    gdf_plot[col]
    .astype("category")
    .cat.add_categories([0])
    .cat.reorder_categories(range(5))
)
gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="hot_r",
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
gdf_plot[col] = (
    gdf_plot[col]
    .astype("category")
    .cat.add_categories([0])
    .cat.reorder_categories(range(7))
)
gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    cmap="hot_r",
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

### IOM rain season impact

```python
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/processed/iom/rainseason_2021_2024.parquet"
df_iom = blob.load_parquet_from_blob(blob_name)
```

```python
df_iom_sum = df_iom.groupby("ADM2_PCODE")["#HH Affected"].sum().reset_index()
```

```python
fig, ax = plt.subplots(dpi=200)

col = "#HH Affected"

gdf_plot = adm2.merge(df_iom_sum)

gdf_plot.plot(
    column=col,
    legend=True,
    ax=ax,
    alpha=0.7,
    cmap="hot_r",
    legend_kwds={"label": "Total Households Affected"},
)
cbar = ax.get_figure().axes[-1]  # Get last axis (colorbar)
cbar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))


adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title("Number of households affected (2021-2024) [IOM]")
```

```python
gdf_plot.sort_values(col, ascending=False)[["ADM1_EN", "ADM2_EN", col]]
```

```python
df_iom_sum["#HH Affected"].quantile(0.9)
```

## Comparison

```python
df_combined = (
    adm2.merge(fs_mean, how="left")
    .merge(df_unicef_count, how="left")
    .merge(df_nema_risk, how="left")
    .merge(df_nihsa_record_sum, how="left")
    .merge(df_iom_sum, how="left")
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
    & (df_combined["Flooded"] >= 2)
].copy()

fig, ax = plt.subplots(dpi=200)

col = "total_exposed"


gdf_plot.plot(column=col, legend=True, ax=ax, cmap="hot_r", vmin=0, vmax=60000)
cbar = ax.get_figure().axes[-1]  # Get last axis (colorbar)
cbar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))


adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")
benue.plot(ax=ax, color="dodgerblue", linewidth=2)

ax.axis("off")
ax.set_title(
    "Mean historical flood exposure in filtered areas\n(riverine flood focus)"
)
```

```python
gdf_plot["Total Exposed"] = gdf_plot["total_exposed"].astype(int)
gdf_plot.sort_values("Total Exposed", ascending=False)[
    ["ADM1_EN", "ADM2_EN", "Total Exposed"]
].rename(
    columns={"ADM2_EN": "LGA", "ADM1_EN": "State"}
).style.background_gradient(
    subset=["Total Exposed"], cmap="hot_r", vmin=0
).format(
    {"Total Exposed": "{:,}"}
)
```

```python
df_combined
```

```python
df_combined[
    (df_combined["#HH Affected"] >= 5000)
    & ((df_combined["riv_num"] >= 2) | (df_combined["flash_num"] >= 2))
    # & (df_combined["Flooded"] >= 2)
]
```

```python
gdf_plot = df_combined[
    (df_combined["#HH Affected"] >= df_combined["#HH Affected"].quantile(0.8))
    & ((df_combined["riv_num"] >= 2) | (df_combined["flash_num"] >= 2))
    & (df_combined["Flooded"] >= 2)
].copy()

fig, ax = plt.subplots(dpi=200)

col = "total_exposed"


gdf_plot.plot(column=col, legend=True, ax=ax, cmap="hot_r", vmin=0, vmax=60000)
cbar = ax.get_figure().axes[-1]  # Get last axis (colorbar)
cbar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))


adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")

ax.axis("off")
ax.set_title(
    "Mean historical flood exposure in filtered areas\n(flash flood focus)"
)
```

```python
gdf_plot["Total Exposed"] = gdf_plot["total_exposed"].astype(int)
gdf_plot.sort_values("Total Exposed", ascending=False)[
    ["ADM1_EN", "ADM2_EN", "Total Exposed"]
].rename(
    columns={"ADM2_EN": "LGA", "ADM1_EN": "State"}
).style.background_gradient(
    subset=["Total Exposed"], cmap="hot_r", vmin=0
).format(
    {"Total Exposed": "{:,}"}
)
```

```python

```
