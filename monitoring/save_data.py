from datetime import datetime, timedelta

import ocha_stratus as stratus
import pandas as pd
import requests
import xarray as xr
from dotenv import load_dotenv

from src.datasources import glofas, grrr
from src.utils import cds_utils

load_dotenv()


def get_blob_name(data_type, station_name, date):
    filename = (
        f"glofas_{station_name}_{data_type}_{date.strftime('%Y-%m-%d')}.grib"
    )
    return f"ds-aa-nga-flooding/raw/glofas/monitoring/{filename}"


def get_coords(station_name):
    station = glofas.GF_STATIONS[station_name]
    glofas_lon, glofas_lat = glofas.get_glofas_grid_coords(
        station["lon"], station["lat"]
    )
    pitch = 0.001
    N = glofas_lat + pitch
    S = glofas_lat
    E = glofas_lon + pitch
    W = glofas_lon
    return [N, W, S, E]


def get_glofas_forecast(
    forecast_blob_name,
    coords,
    issued_date,
    keep_local_copy=True,
    overwrite=False,
):
    container = stratus.get_container_client("projects", "dev")
    if (
        container.get_blob_client(forecast_blob_name).exists()
        and not overwrite
    ):
        print(f"File already exists: {forecast_blob_name}. Skipping download")
        return
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
        keep_local_copy=keep_local_copy,
    )


def get_glofas_reanalysis(
    reanalysis_blob_name,
    coords,
    issued_date,
    keep_local_copy=True,
    overwrite=False,
):
    container = stratus.get_container_client("projects", "dev")
    if (
        container.get_blob_client(reanalysis_blob_name).exists()
        and not overwrite
    ):
        print(
            f"File already exists: {reanalysis_blob_name}. Skipping download"
        )
        return
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
        keep_local_copy=keep_local_copy,
    )


def get_google_forecast(hybas_id, issued_date):
    res = requests.get(
        "https://floodforecasting.googleapis.com/v1/gauges:queryGaugeForecasts",  # noqa
        params={
            "key": grrr.GOOGLE_API_KEY,
            "gaugeIds": hybas_id,
            "issuedTimeStart": issued_date.strftime("%Y-%m-%d"),
        },
    ).json()

    rows = []

    for forecast in res["forecasts"][hybas_id]["forecasts"]:
        issued_time = forecast["issuedTime"]
        gauge_id = forecast["gaugeId"]

        for range_item in forecast["forecastRanges"]:
            row = {
                "issued_time": issued_time,
                "valid_time": range_item["forecastStartTime"],
                "value": range_item["value"],
                "src": f"grrr_{gauge_id}",
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    # In case of multiple forecasts issued from the same day, we want to keep
    # the one that was issued latest
    df = df[df.issued_time == df.issued_time.max()]
    return df


def process_glofas(blob_name, data_type, station_name):
    ds = xr.open_dataset(
        f"temp/{blob_name}",
        engine="cfgrib",
        decode_timedelta=True,
        backend_kwargs={
            "indexpath": "",
        },
    )
    df = (
        ds.assign_coords(valid_time=ds["valid_time"] - pd.Timedelta(hours=24))
        .to_dataframe()
        .reset_index()
    )
    df["valid_time"] = pd.to_datetime(df["valid_time"])
    df["src"] = f"{data_type}_{station_name}"
    df = df.rename(columns={"dis24": "value", "time": "issued_time"})
    return df[["issued_time", "valid_time", "value", "src"]]


if __name__ == "__main__":
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)
    station_name = "wuroboki"

    coords = get_coords(station_name)
    forecast_blob_name = get_blob_name("forecast", station_name, today)
    # NOTE that we're saving the reanalysis data based on the day it was
    # MONITORED and NOT for the day that it is valid!
    reanalysis_blob_name = get_blob_name("reanalysis", station_name, today)

    # --- 1. Saving raw GloFAS data...
    get_glofas_forecast(forecast_blob_name, coords, today, overwrite=True)
    get_glofas_reanalysis(
        reanalysis_blob_name, coords, two_days_ago, overwrite=True
    )

    # --- 2. Get the Glofas dataframes...
    df_forecast = process_glofas(
        forecast_blob_name, "glofas_forecast", station_name
    )
    df_reanalysis = process_glofas(
        reanalysis_blob_name, "glofas_reanalysis", station_name
    )

    # --- 3. Getting Google dataframe...
    df_google = get_google_forecast(grrr.HYBAS_ID, today)

    # --- 4. Combine and save to database...
    df_all = pd.concat([df_forecast, df_reanalysis, df_google])
    df_all["updated"] = datetime.now()
    engine = stratus.get_engine(stage="dev", write=True)
    df_all.to_sql(
        "nga_cerf_flooding",
        schema="monitoring",
        con=engine,
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
    )
    print(f"{len(df_all)} rows saved to database!")
