---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.7
  kernelspec:
    display_name: venv
    language: python
    name: python3
---

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os
import pandas as pd
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import xskillscore as xs
import folium
from src.datasources import hydrosheds, codab, glofas, nihsa
from src.utils import rp_calc
from dotenv import load_dotenv
from src.constants import WUROBOKI_LAT, WUROBOKI_LON

load_dotenv()

HYBAS_ID = "hybas_1120842550"
HYBAS_ID_ = "hybas_1120842990"  # Right at Wuro Boki, but lower confidence
BENUE_ADM2_PCODES = ["NG002016", "NG002009", "NG002021", "NG002005"]
TARGET_RP = 5

gdf_benue = hydrosheds.load_benue_aoi()
gdf_adm = codab.load_codab(1, aoi_only=True)
```

```python
def open_zarr(path):
    return xr.open_zarr(
        store=path, chunks="auto", storage_options=dict(token="anon")
    )
```

## Load in the GRRR data

```python
base_directory = (
    "gs://flood-forecasting/hydrologic_predictions/model_id_8583a5c2_v0/"
)
reforecast_path = os.path.join(base_directory, "reforecast/streamflow.zarr/")
reanalysis_path = os.path.join(base_directory, "reanalysis/streamflow.zarr/")
return_periods_path = os.path.join(base_directory, "return_periods.zarr/")
outlets_path = os.path.join(
    base_directory, "hybas_outlet_locations_UNOFFICIAL.zarr/"
)
```

```python
ds_reforecast_grrr = (
    open_zarr(reforecast_path).sel(gauge_id=HYBAS_ID).compute()
)
ds_reanalysis_grrr = (
    open_zarr(reanalysis_path).sel(gauge_id=HYBAS_ID).compute()
)
ds_outlet_location_grrr = (
    open_zarr(outlets_path).sel(gauge_id=HYBAS_ID).compute()
)
ds_return_periods_grrr = (
    open_zarr(return_periods_path).sel(gauge_id=HYBAS_ID).compute()
)
```

```python
# Clean up reforecast and convert to dataframe
df_reforecast_grrr = ds_reforecast_grrr.to_dataframe().reset_index()

df_reforecast_grrr["valid_time"] = df_reforecast_grrr.apply(
    lambda row: row["issue_time"] + row["lead_time"], axis=1
)
df_reforecast_grrr["leadtime"] = df_reforecast_grrr["lead_time"].apply(
    lambda x: x.days
)
df_reforecast_grrr = df_reforecast_grrr.drop(columns=["lead_time", "gauge_id"])

# Clean up reanalysis and convert to dataframe
df_reanalysis_grrr = ds_reanalysis_grrr.to_dataframe().reset_index()
df_reanalysis_grrr = df_reanalysis_grrr.rename(
    columns={"time": "valid_time", "streamflow": "sf_grrr_reanalysis"}
).drop(columns=["gauge_id"])
```

### Return Periods


Get the Google-derived return periods

```python
return_periods_dict = {}

for var_name in ds_return_periods_grrr.data_vars:
    return_periods_dict[int(var_name.split("_")[-1])] = ds_return_periods_grrr[
        var_name
    ].item()

df_rp_grrr = pd.DataFrame([return_periods_dict])
df_rp_grrr = (
    df_rp_grrr.T.reset_index()
    .rename(columns={"index": "return_period", 0: "streamflow"})
    .sort_values("return_period", ascending=True)
)
```

Now estimate return periods directly from the reanalysis data

```python
return_periods = df_rp_grrr["return_period"]
df_rp_calculated = rp_calc.estimate_return_periods(
    df_reanalysis_grrr,
    date_col="valid_time",
    val_col="sf_grrr_reanalysis",
    target_rps=list(return_periods),
)
```

Now plot both return periods

```python
plt.plot(
    df_rp_grrr["return_period"],
    df_rp_grrr["streamflow"],
    "o-",
    label="Google Return Periods",
)
plt.plot(
    df_rp_calculated["return_period"],
    df_rp_calculated["value"],
    "o-",
    label="Calculated Return Periods from Reanalysis",
)
plt.xlabel("Return Period")
plt.ylabel("Streamflow (m$^3$/s)")
plt.title("Streamflow return periods from GRRR product")
plt.legend()
plt.savefig(
    f"temp/{HYBAS_ID}_return_periods.png", dpi=300, bbox_inches="tight"
)
plt.show()
```

## Skill against reforecast

How well does the forecast match the model?

```python
fig1, ax1 = plt.subplots()
fig2, ax2 = plt.subplots()


def percent_bias(obs, pred):
    return 100.0 * sum(pred - obs) / sum(obs)


df_merged = df_reforecast_grrr.merge(df_reanalysis_grrr, how="left").rename(
    columns={
        "streamflow": "sf_reforecast",
        "sf_grrr_reanalysis": "sf_reanalysis",
    }
)

for rp in [None, 2, 5, 7]:
    if rp:
        thresh = float(
            df_rp_calculated.loc[
                df_rp_calculated["return_period"] == rp, "value"
            ].iloc[0]
        )

        df_merged = df_merged[df_merged["sf_reforecast"] >= thresh]

    df_bias = (
        df_merged.groupby("leadtime")
        .apply(
            lambda x: percent_bias(x["sf_reanalysis"], x["sf_reforecast"]),
            include_groups=False,
        )
        .reset_index(name="percent_bias")
    )

    dimension_cols = ["issue_time", "valid_time", "leadtime"]
    data_vars = ["sf_reforecast", "sf_reanalysis"]

    indexed_df = df_merged.set_index(dimension_cols)
    ds = indexed_df[data_vars].to_xarray()

    df_skill = (
        ds.xs.mape(
            "sf_reanalysis",
            "sf_reforecast",
            dim=["issue_time", "valid_time"],
            skipna=True,
        )
        .to_series()
        .reset_index()
        .rename(columns={0: "mape"})
    )
    label = f">= 1 in {rp}-year" if rp else "All"

    ax1.plot(
        df_skill["leadtime"],
        df_skill["mape"] * 100,
        label=label,
    )

    ax2.plot(
        df_bias["leadtime"],
        df_bias["percent_bias"],
        label=label,
    )

ax1.legend()
ax1.set_xlabel("Leadtime (days)")
ax1.set_ylabel("Mean Absolute Percent Error (%)")
ax1.set_title("Forecast error across leadtimes")

fig1.savefig(
    f"temp/{HYBAS_ID}_forecast_mape.png", dpi=300, bbox_inches="tight"
)

ax2.legend()
ax2.axhline(y=0, color="r", linestyle="--", alpha=0.7)
ax2.set_xlabel("Leadtime (days)")
ax2.set_ylabel("Percent Bias (%)")
ax2.set_title("Forecast percent bias by lead time")

fig2.savefig(
    f"temp/{HYBAS_ID}_forecast_bias.png", dpi=300, bbox_inches="tight"
)
```

## Load in the observational NIHSA data

```python
df_observational_nihsa = nihsa.load_wuroboki().rename(
    columns={"time": "valid_time", "level": "level"}
)
```

How well is the GRRR reanalysis data correlated with observational water levels at Wuro Boki?

```python
df_merged = (
    df_observational_nihsa.merge(df_reanalysis_grrr, how="left")
    .dropna()
    .rename(columns={"sf_grrr_reanalysis": "streamflow"})
)
```

Let's first break down by year

```python
station_name = "Wuroboki"

dis_max = df_merged["streamflow"].max()
level_max = df_merged["level"].max()

n_years = df_merged["valid_time"].dt.year.nunique()

ncols = 4
nrows = round(n_years / ncols)

fig, axes = plt.subplots(
    nrows=nrows, ncols=ncols, figsize=(ncols * 5, nrows * 3)
)
axes = axes.flatten()

year_correlations = {}

for j, year in enumerate(df_merged["valid_time"].dt.year.unique()):
    dff = df_merged[df_merged["valid_time"].dt.year == year]
    ax = axes[j]
    ax2 = ax.twinx()

    ax.plot(
        dff["valid_time"],
        dff["level"],
        color="dodgerblue",
        label="NiHSA\n(mm, left axis)",
    )
    ax.set_ylim(bottom=0, top=level_max * 1.1)

    ax2.plot(
        dff["valid_time"],
        dff["streamflow"],
        color="darkorange",
        label="GRRR\n(m$^{3}$/s, right axis)",
    )

    ax2.set_ylim(bottom=0, top=dis_max * 1.1)

    pearson_r = dff["level"].corr(dff["streamflow"], method="pearson")
    spearman_r = dff["level"].corr(dff["streamflow"], method="spearman")

    year_correlations[year] = {
        "pearson_r": pearson_r,
        "spearman_r": spearman_r,
    }
    if pearson_r < 0.5 and spearman_r < 0.5:
        bbox_col = "#ffa694"  # Roughly which ones have bad relationship
    else:
        bbox_col = "white"

    ax.text(
        0.05,
        0.95,
        f"Pearson r: {pearson_r:.2f}\nSpearman r: {spearman_r:.2f}",
        transform=ax.transAxes,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor=bbox_col),
    )

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.set_title(year)

fig.suptitle(
    f"Comparison of NiHSA and GRRR reanalysis for {station_name.capitalize()}",
    fontsize=16,
    y=1,
)
plt.savefig(
    f"temp/{HYBAS_ID}_nihsa_correlation.png", dpi=300, bbox_inches="tight"
)
plt.tight_layout()
```

Now look at overall

```python
pearson_corr = df_merged["level"].corr(
    df_merged["streamflow"], method="pearson"
)
spearman_corr = df_merged["level"].corr(
    df_merged["streamflow"], method="spearman"
)
print(f"\nOverall Pearson correlation: {pearson_corr:.4f}")
print(f"Overall Spearman correlation: {spearman_corr:.4f}")

plt.figure(figsize=(10, 6))
plt.scatter(df_merged["streamflow"], df_merged["level"], alpha=0.1)
plt.title("Water Level vs Streamflow")
plt.xlabel("Streamflow (m$^{3}$/s)")
plt.ylabel("Water level")

plt.savefig(
    f"temp/{HYBAS_ID}_nihsa_correlation_overall.png",
    dpi=300,
    bbox_inches="tight",
)

plt.show()
```

## Return period exceedance

```python
# First let's only take the observational data since 1980
df_observational_nihsa = df_observational_nihsa[
    df_observational_nihsa["valid_time"] > "1980-01-01"
]
```

```python
df_rp_calculated_nihsa = rp_calc.estimate_return_periods(
    df_observational_nihsa,
    date_col="valid_time",
    val_col="level",
    target_rps=list(return_periods),
)
```

For the observational data

```python
rp_2 = rp_calc.get_rp_val(df_rp_calculated_nihsa, 2)
rp_5 = rp_calc.get_rp_val(df_rp_calculated_nihsa, 5)
rp_7 = rp_calc.get_rp_val(df_rp_calculated_nihsa, 7)

fig, ax = plt.subplots(figsize=(20, 6))

df_observational_nihsa.plot(x="valid_time", y="level", ax=ax, c="black")
ax.axhline(rp_2, c="blue", label="1 in 2 rp")
ax.axhline(rp_5, c="orange", label="1 in 5 rp")
ax.axhline(rp_7, c="red", label="1 in 7 rp")

mask_rp2 = df_observational_nihsa["level"] >= rp_2
mask_rp5 = df_observational_nihsa["level"] >= rp_5
mask_rp7 = df_observational_nihsa["level"] >= rp_7

ax.scatter(
    df_observational_nihsa.loc[mask_rp2 & ~mask_rp5, "valid_time"],
    df_observational_nihsa.loc[mask_rp2 & ~mask_rp5, "level"],
    c="blue",
    s=30,
    label="Exceeds 1 in 2 rp",
)
ax.scatter(
    df_observational_nihsa.loc[mask_rp5 & ~mask_rp7, "valid_time"],
    df_observational_nihsa.loc[mask_rp5 & ~mask_rp7, "level"],
    c="orange",
    s=30,
    label="Exceeds 1 in 5 rp",
)
ax.scatter(
    df_observational_nihsa.loc[mask_rp7, "valid_time"],
    df_observational_nihsa.loc[mask_rp7, "level"],
    c="red",
    s=30,
    label="Exceeds 1 in 7 rp",
)
plt.legend()
```

```python
# Get the date ranges when water level goes above a given rp
df_rp5_mask = df_observational_nihsa[mask_rp5]
```

```python
def find_date_ranges(df, date_column="valid_time"):
    dates = (
        pd.to_datetime(df[date_column]).sort_values().reset_index(drop=True)
    )
    breaks = np.where(np.diff(dates) > pd.Timedelta(days=1))[0]
    indices = np.concatenate([[0], breaks + 1, [len(dates)]])
    return [
        dates[indices[i] : indices[i + 1]].tolist()
        for i in range(len(indices) - 1)
    ]
```

```python
find_date_ranges(df_rp5_mask)
```

For the reanalysis data

```python
rp_2 = rp_calc.get_rp_val(df_rp_calculated, 2)
rp_5 = rp_calc.get_rp_val(df_rp_calculated, 5)
rp_7 = rp_calc.get_rp_val(df_rp_calculated, 7)

fig, ax = plt.subplots(figsize=(20, 6))

df_reanalysis_grrr.plot(
    x="valid_time", y="sf_grrr_reanalysis", ax=ax, c="black"
)
ax.axhline(rp_2, c="blue", label="1 in 2 rp")
ax.axhline(rp_5, c="orange", label="1 in 5 rp")
ax.axhline(rp_7, c="red", label="1 in 7 rp")

mask_rp2 = df_reanalysis_grrr["sf_grrr_reanalysis"] >= rp_2
mask_rp5 = df_reanalysis_grrr["sf_grrr_reanalysis"] >= rp_5
mask_rp7 = df_reanalysis_grrr["sf_grrr_reanalysis"] >= rp_7

ax.scatter(
    df_reanalysis_grrr.loc[mask_rp2 & ~mask_rp5, "valid_time"],
    df_reanalysis_grrr.loc[mask_rp2 & ~mask_rp5, "sf_grrr_reanalysis"],
    c="blue",
    s=30,
    label="Exceeds 1 in 2 rp",
)
ax.scatter(
    df_reanalysis_grrr.loc[mask_rp5 & ~mask_rp7, "valid_time"],
    df_reanalysis_grrr.loc[mask_rp5 & ~mask_rp7, "sf_grrr_reanalysis"],
    c="orange",
    s=30,
    label="Exceeds 1 in 5 rp",
)
ax.scatter(
    df_reanalysis_grrr.loc[mask_rp7, "valid_time"],
    df_reanalysis_grrr.loc[mask_rp7, "sf_grrr_reanalysis"],
    c="red",
    s=30,
    label="Exceeds 1 in 7 rp",
)
plt.legend()
```

```python
rp_2
```

## Load in the GloFAS data

```python
df_reforecast_glofas = glofas.load_reforecast().rename(
    columns={"time": "issued_time"}
)
df_reanalysis_glofas = glofas.load_reanalysis().rename(
    columns={"time": "valid_time"}
)
```

## Plot locations

```python
grrr_lat = float(ds_outlet_location_grrr.latitude.values)
grrr_lon = float(ds_outlet_location_grrr.longitude.values)

m = folium.Map(
    location=[grrr_lat, grrr_lon], zoom_start=12, tiles="CartoDB positron"
)
locations = [
    {"name": "GRRR HYBAS Location", "lat": grrr_lat, "lon": grrr_lon},
    {
        "name": "GLOFAS Wuroboki Point",
        "lat": WUROBOKI_LAT,
        "lon": WUROBOKI_LON,
    },
]

for loc in locations:
    folium.Marker(
        location=[loc["lat"], loc["lon"]],
        popup=loc["name"],
        tooltip=loc["name"],
    ).add_to(m)

folium.GeoJson(
    gdf_benue,
    name="Shapefile Layer",
    style_function=lambda x: {
        "fillColor": "#ffff00",
        "color": "#000000",
        "fillOpacity": 0.5,
        "weight": 1.5,
    },
).add_to(m)

m
```
