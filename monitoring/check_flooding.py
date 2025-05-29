from datetime import datetime, timedelta

import cdsapi
import matplotlib.pyplot as plt
import pandas as pd
import requests
import xarray as xr

from src.datasources import glofas, grrr
from src.utils import cds_utils

GLOFAS_THRESH = 3130
GOOGLE_THRESH = 1212


def get_glofas_forecast(forecast_blob_name, coords, issued_date):
    forecast_dataset = "cems-glofas-forecast"
    forecast_request = {
        "system_version": ["operational"],
        "hydrological_model": ["lisflood"],
        "product_type": ["control_forecast"],
        "variable": "river_discharge_in_the_last_24_hours",
        "year": [str(issued_date.year)],
        "month": [str(issued_date.month).zfill(2)],
        "day": [str(issued_date.day).zfill(2)],
        "leadtime_hour": [
            "24",
            "48",
            "72",
            "96",
            "120",
        ],
        "data_format": "grib2",
        "download_format": "unarchived",
        "area": coords,
    }

    cds_utils.download_raw_cds_api_to_blob(
        forecast_dataset,
        forecast_request,
        forecast_blob_name,
        keep_local_copy=True,
    )


def get_glofas_reanalysis(reanalysis_blob_name, coords, issued_date):
    reanalysis_dataset = "cems-glofas-historical"
    reanalysis_request = {
        "system_version": ["version_4_0"],
        "hydrological_model": ["lisflood"],
        "product_type": ["intermediate"],
        "variable": ["river_discharge_in_the_last_24_hours"],
        "hyear": [str(issued_date.year)],
        "hmonth": [str(issued_date.month).zfill(2)],
        "hday": [str(issued_date.day).zfill(2)],
        "data_format": "grib2",
        "download_format": "unarchived",
        "area": coords,
    }
    cds_utils.download_raw_cds_api_to_blob(
        reanalysis_dataset,
        reanalysis_request,
        reanalysis_blob_name,
        keep_local_copy=True,
    )


def process_glofas_reanalysis(reanalysis_blob_name):
    ds_reanalysis = xr.open_dataset(
        f"temp/{reanalysis_blob_name}",
        engine="cfgrib",
        decode_timedelta=True,
        backend_kwargs={
            "indexpath": "",
        },
    )
    ds_reanalysis = ds_reanalysis.assign_coords(
        valid_time_start=ds_reanalysis["valid_time"] - pd.Timedelta(hours=24)
    )
    df_reanalysis = ds_reanalysis.to_dataframe().reset_index()
    df_reanalysis["valid_time"] = pd.to_datetime(df_reanalysis["valid_time"])
    return df_reanalysis


def process_glofas_forecast(forecast_blob_name):
    ds_forecast = xr.open_dataset(
        f"temp/{forecast_blob_name}",
        engine="cfgrib",
        decode_timedelta=True,
        backend_kwargs={
            "indexpath": "",
        },
    )
    ds_forecast = ds_forecast.assign_coords(
        valid_time_start=ds_forecast["valid_time"] - pd.Timedelta(hours=24)
    )
    df_forecast = ds_forecast.to_dataframe().reset_index()
    df_forecast["valid_time"] = pd.to_datetime(df_forecast["valid_time"])
    return df_forecast


def get_google_forecast(hybas_id, issued_date):
    res = requests.get(
        "https://floodforecasting.googleapis.com/v1/gauges:queryGaugeForecasts",
        params={
            "key": grrr.FLOODS_API_KEY,
            "gaugeIds": hybas_id,
            "issuedTimeStart": issued_date.strftime("%Y-%m-%d"),
        },
    ).json()

    rows = []

    for forecast in res["forecasts"][hybas_id]["forecasts"]:
        issued_time = forecast["issuedTime"]
        gauge_id = forecast["gaugeId"]

        # Process each forecast range
        for range_item in forecast["forecastRanges"]:
            row = {
                "issuedTime": issued_time,
                "gaugeId": gauge_id,
                "value": range_item["value"],
                "forecastStartTime": range_item["forecastStartTime"],
                "forecastEndTime": range_item["forecastEndTime"],
            }
            rows.append(row)

    # Create the DataFrame
    df = pd.DataFrame(rows)

    # Convert string timestamps to datetime objects for easier analysis
    df["issuedTime"] = pd.to_datetime(df["issuedTime"])
    df["forecastStartTime"] = pd.to_datetime(df["forecastStartTime"])
    df["forecastEndTime"] = pd.to_datetime(df["forecastEndTime"])

    # In case there are two forecasts, get the latest one
    df = df[df.issuedTime == df.issuedTime.max()]
    return df


if __name__ == "__main__":
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)

    c = cdsapi.Client()

    # Get the pixel location
    station = glofas.GF_STATIONS["wuroboki"]
    glofas_lon, glofas_lat = glofas.get_glofas_grid_coords(
        station["lon"], station["lat"]
    )
    pitch = 0.001
    N = glofas_lat + pitch
    S = glofas_lat
    E = glofas_lon + pitch
    W = glofas_lon
    coords = [N, W, S, E]

    # NOTE that we're saving the data with the date of MONITORING in the filename, not the date for which it is valid
    reanalysis_filename = f"glofas_reanalysis_{today.year}-{str(today.month).zfill(2)}-{str(today.day).zfill(2)}.grib"
    reanalysis_blob_name = (
        f"ds-aa-nga-flooding/raw/glofas/monitoring/{reanalysis_filename}"
    )

    forecast_filename = f"glofas_forecast_{today.year}-{str(today.month).zfill(2)}-{str(today.day).zfill(2)}.grib"
    forecast_blob_name = (
        f"ds-aa-nga-flooding/raw/glofas/monitoring/{forecast_filename}"
    )

    # --- 1. Download the GloFAS data
    get_glofas_forecast(forecast_blob_name, coords, today)
    get_glofas_reanalysis(reanalysis_blob_name, coords, two_days_ago)

    # --- 2. Process the GloFAS data
    df_forecast = process_glofas_forecast(forecast_blob_name)
    df_reanalysis = process_glofas_reanalysis(reanalysis_blob_name)

    # --- 3. Download and process Google data
    df_google = get_google_forecast(grrr.HYBAS_ID, today)

    # --- 4. Check conditions
    forecast_exceeds = bool((df_forecast["dis24"] > GLOFAS_THRESH).any())
    reanalysis_exceeds = bool((df_reanalysis["dis24"] > GLOFAS_THRESH).any())

    glofas_exceeds = forecast_exceeds | reanalysis_exceeds
    google_exceeds = bool((df_google["value"] > GOOGLE_THRESH).any())

    overall_exceeds = glofas_exceeds | google_exceeds

    # --- 5. Create plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # ============ FIRST SUBPLOT - GloFAS Data ============
    ax1.plot(
        df_forecast["valid_time_start"],
        df_forecast["dis24"],
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=4,
        label="Forecast",
        color="blue",
        alpha=0.8,
    )

    for i, row in df_forecast.iterrows():
        ax1.annotate(
            f'{row["dis24"]:.1f}',
            (row["valid_time_start"], row["dis24"]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            color="blue",
        )

    ax1.plot(
        df_reanalysis["valid_time_start"],
        df_reanalysis["dis24"],
        marker="s",
        linestyle="-",
        linewidth=2,
        markersize=6,
        label="Reanalysis",
        color="red",
        alpha=0.8,
    )

    # Add labels for reanalysis points
    for i, row in df_reanalysis.iterrows():
        ax1.annotate(
            f'{row["dis24"]:.1f}',
            (row["valid_time_start"], row["dis24"]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            color="red",
        )

    # Check if values exceed threshold and create title
    forecast_exceeds = (df_forecast["dis24"] > GLOFAS_THRESH).any()
    reanalysis_exceeds = (df_reanalysis["dis24"] > GLOFAS_THRESH).any()

    title1 = f"GloFAS Monitoring: {today} | Triggers = {glofas_exceeds}"

    # Add horizontal threshold line
    ax1.axhline(
        y=GLOFAS_THRESH,
        color="black",
        linestyle="--",
        linewidth=2,
        label=f"Trigger Threshold ({GLOFAS_THRESH})",
        alpha=0.8,
    )

    ax1.set_ylabel("Streamflow", fontsize=12)
    ax1.set_title(title1, fontsize=12, fontweight="bold")
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)

    # ============ SECOND SUBPLOT - Google Data ============

    ax2.plot(
        df_google["forecastStartTime"],
        df_google["value"],
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=4,
        label="Forecast",
        color="blue",
        alpha=0.8,
    )

    for i, row in df_google.iterrows():
        ax2.annotate(
            f'{row["value"]:.1f}',
            (row["forecastStartTime"], row["value"]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            color="blue",
        )

    ax2.axhline(
        y=GOOGLE_THRESH,
        color="black",
        linestyle="--",
        linewidth=2,
        label=f"Trigger Threshold ({GOOGLE_THRESH})",
        alpha=0.8,
    )

    title2 = f"Google Monitoring: {today} | Triggers = {google_exceeds}"

    # Check if values exceed threshold and create title
    value_exceeds = (df_google["value"] > GLOFAS_THRESH).any()

    ax2.set_xlabel("Valid Date", fontsize=12)
    ax2.set_ylabel("Streamflow", fontsize=12)
    ax2.set_title(title2, fontsize=12, fontweight="bold")
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(f"temp/{today}_{overall_exceeds}.png", dpi=300)
