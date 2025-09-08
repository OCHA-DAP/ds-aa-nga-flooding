from datetime import timedelta

import ocha_stratus as stratus
import pandas as pd
import requests
import xarray as xr
from dotenv import load_dotenv
from sqlalchemy import text

from src.constants import (
    GLOFAS_THRESH,
    GLOFAS_WARNING_THRESH,
    GOOGLE_THRESH,
    GOOGLE_WARNING_THRESH,
)
from src.datasources import grrr
from src.utils import cds_utils

load_dotenv()

DB_SCHEMA = "projects"
DB_TABLE = "ds_aa_nga_flooding_monitoring"


def get_blob_name(data_type, station_name, date):
    filename = (
        f"glofas_{station_name}_{data_type}_{date.strftime('%Y-%m-%d')}.grib"
    )
    return f"ds-aa-nga-flooding/raw/glofas/monitoring/{filename}"


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
        "product_type": ["ensemble_perturbed_forecasts"],
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
            "issuedTimeEnd": (issued_date + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        },
    ).json()

    rows = []

    for forecast in res["forecasts"][hybas_id]["forecasts"]:
        issued_time = forecast["issuedTime"]
        gauge_id = forecast["gaugeId"]

        for range_item in forecast["forecastRanges"]:
            row = {
                "issued_time": issued_time,
                "valid_date": range_item["forecastStartTime"],
                "value": range_item["value"],
                "src": f"grrr_{gauge_id}",
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    # In case of multiple forecasts issued from the same day, we want to keep
    # the one that was issued latest
    df = df[df.issued_time == df.issued_time.max()]

    # Make sure it's all in utc time
    df["issued_time"] = pd.to_datetime(df["issued_time"], utc=True)
    df["issued_date"] = df["issued_time"]
    df["valid_date"] = pd.to_datetime(df["valid_date"], utc=True)
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
    # Take the ensemble mean if forecast
    if data_type == "glofas_forecast":
        ds = ds["dis24"].mean(dim="number")
    df = (
        ds.assign_coords(valid_time=ds["valid_time"] - pd.Timedelta(hours=24))
        .to_dataframe()
        .reset_index()
    )
    df["valid_date"] = pd.to_datetime(df["valid_time"])
    df["src"] = f"{data_type}_{station_name}"
    df = df.rename(columns={"dis24": "value", "time": "issued_date"})
    return df[["issued_date", "valid_date", "value", "src"]]


def get_database_forecast(monitoring_date):
    engine = stratus.get_engine(stage="dev")
    with engine.connect() as con:
        df = pd.read_sql(
            text(
                f"""
            select * from {DB_SCHEMA}.{DB_TABLE}
            where monitoring_date = :monitoring_date
            order by valid_date
            """
            ),
            con=con,
            params={"monitoring_date": monitoring_date},
        )
    if len(df) == 0:
        raise Exception(f"No data saved for {monitoring_date}")
    return df


def check_results(monitoring_date, activation=True):
    if activation:
        google_thresh = GOOGLE_THRESH
        glofas_thresh = GLOFAS_THRESH
    else:
        google_thresh = GOOGLE_WARNING_THRESH
        glofas_thresh = GLOFAS_WARNING_THRESH

    df = get_database_forecast(monitoring_date)
    assert df.monitoring_date.nunique() == 1

    df_forecast = df[df.src.str.contains("glofas_forecast")].reset_index()
    df_reanalysis = df[df.src.str.contains("glofas_reanalysis")].reset_index()
    df_google = df[df.src.str.contains("grrr_hybas")].reset_index()

    glofas_exceeds = (df_reanalysis.value > glofas_thresh).any() | (
        df_forecast.value > glofas_thresh
    ).any()
    google_exceeds = (df_google.value > google_thresh).any()
    overall_exceeds = glofas_exceeds | google_exceeds
    return overall_exceeds
