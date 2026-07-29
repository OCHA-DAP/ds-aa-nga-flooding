from datetime import timedelta

import ocha_stratus as stratus
import pandas as pd
import requests
import xarray as xr
from dotenv import load_dotenv
from sqlalchemy import text

from src.constants import (
    ACTION_GAUGE_THRESHOLDS,
    ACTION_MIN_GAUGES,
    READINESS_GLOFAS_THRESH,
    READINESS_MAX_LEADTIME,
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
        # Leads out to the readiness trigger's max lead time
        "leadtime_hour": [
            str(24 * lead)
            for lead in range(1, READINESS_MAX_LEADTIME + 1)
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


def get_google_forecasts(gauge_ids, issued_date):
    """Get the latest Google forecast issued on ``issued_date`` for each
    gauge in ``gauge_ids``."""
    res = requests.get(
        "https://floodforecasting.googleapis.com/v1/gauges:queryGaugeForecasts",  # noqa
        params={
            "key": grrr.GOOGLE_API_KEY,
            "gaugeIds": list(gauge_ids),
            "issuedTimeStart": issued_date.strftime("%Y-%m-%d"),
            "issuedTimeEnd": (issued_date + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        },
    ).json()

    forecasts = res.get("forecasts", {})
    missing = [g for g in gauge_ids if g not in forecasts]
    if missing:
        print(f"WARNING: no forecast returned for gauges: {missing}")

    rows = []
    for gauge_id, gauge_forecasts in forecasts.items():
        for forecast in gauge_forecasts["forecasts"]:
            for range_item in forecast["forecastRanges"]:
                rows.append(
                    {
                        "issued_time": forecast["issuedTime"],
                        "valid_date": range_item["forecastStartTime"],
                        "value": range_item["value"],
                        "src": f"grrr_{forecast['gaugeId']}",
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # In case of multiple forecasts issued from the same day, we want to keep
    # the one that was issued latest (per gauge)
    df = df[
        df.issued_time == df.groupby("src")["issued_time"].transform("max")
    ]

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


def evaluate_trigger(df):
    """Evaluate the 2026 framework triggers from one monitoring day's
    forecasts.

    - Action: >= ACTION_MIN_GAUGES of the selected GRRR gauges exceed their
      individual 4-yr RP threshold on the same forecast valid day.
    - Readiness: GloFAS ensemble-mean forecast at Wuroboki exceeds
      READINESS_GLOFAS_THRESH at a lead time <= READINESS_MAX_LEADTIME days,
      OR the latest GloFAS reanalysis at Wuroboki exceeds the same threshold
      (near-real-time supplementary condition).
    """
    assert df.monitoring_date.nunique() == 1

    df_glofas = df[df.src.str.contains("glofas_forecast")].copy()
    df_reanalysis = df[df.src.str.contains("glofas_reanalysis")].copy()
    df_google = df[df.src.str.startswith("grrr_")].copy()

    # Coerce as the database returns plain dates
    leadtime_days = (
        pd.to_datetime(df_glofas.valid_date)
        - pd.to_datetime(df_glofas.issued_date)
    ).dt.days
    df_glofas = df_glofas[leadtime_days <= READINESS_MAX_LEADTIME]
    readiness_forecast = bool(
        (df_glofas.value > READINESS_GLOFAS_THRESH).any()
    )
    readiness_reanalysis = bool(
        (df_reanalysis.value > READINESS_GLOFAS_THRESH).any()
    )
    readiness = readiness_forecast | readiness_reanalysis

    df_google["threshold"] = (
        df_google.src.str.removeprefix("grrr_").map(ACTION_GAUGE_THRESHOLDS)
    )
    df_google = df_google.dropna(subset=["threshold"])
    df_google["exceeds"] = df_google.value > df_google.threshold
    daily_counts = df_google.groupby("valid_date")["exceeds"].sum()
    max_gauges = int(daily_counts.max()) if len(daily_counts) else 0

    return {
        "action": max_gauges >= ACTION_MIN_GAUGES,
        "readiness": readiness,
        "readiness_forecast": readiness_forecast,
        "readiness_reanalysis": readiness_reanalysis,
        "max_gauges_exceeding": max_gauges,
        "max_gauges_date": (
            daily_counts.idxmax() if len(daily_counts) else None
        ),
        "n_gauges_reporting": df_google.src.nunique(),
        "glofas_max": (
            float(df_glofas.value.max()) if len(df_glofas) else None
        ),
        "reanalysis_max": (
            float(df_reanalysis.value.max()) if len(df_reanalysis) else None
        ),
    }


def check_trigger_status(monitoring_date):
    return evaluate_trigger(get_database_forecast(monitoring_date))
