from datetime import datetime, timedelta

import ocha_stratus as stratus

from src.datasources import glofas
from src.utils import cds_utils


def get_blob_name(data_type, date):
    filename = f"glofas_{data_type}_{date.strftime('%Y-%m-%d')}.grib"
    return f"ds-aa-nga-flooding/raw/glofas/monitoring/{filename}"


def get_coords(station_name="wurobokki"):
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
    leadtimes=None,
    keep_local_copy=True,
    overwrite=False,
):
    if not leadtimes:
        leadtimes = (["24", "48", "72", "96", "120"],)

    container = stratus.get_container_client("projects", "dev")
    if (
        container.get_blob_client(forecast_blob_name).exists()
        and not overwrite
    ):
        print("File already exists. Skipping download")
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
        "leadtime_hour": leadtimes,
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
        container.get_blob_client(forecast_blob_name).exists()
        and not overwrite
    ):
        print("File already exists. Skipping download")
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


if __name__ == "__main__":
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)

    coords = get_coords("wuroboki")
    forecast_blob_name = get_blob_name("forecast", today)
    reanalysis_blob_name = get_blob_name("reanalysis", two_days_ago)

    get_glofas_forecast(forecast_blob_name, coords, today)
    get_glofas_reanalysis(reanalysis_blob_name, coords, two_days_ago)
