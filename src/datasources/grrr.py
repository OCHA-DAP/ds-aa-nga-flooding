import os
from datetime import datetime, timedelta
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests
import xarray as xr
from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

HYBAS_ID = "hybas_1120842550"
BASE_DIRECTORY = (
    "gs://flood-forecasting/hydrologic_predictions/model_id_8583a5c2_v0/"
)


def open_zarr(path):
    return xr.open_zarr(
        store=path, chunks="auto", storage_options=dict(token="anon")
    )


def load_reanalysis(gauge=HYBAS_ID):
    """Load reanalysis streamflow for one or more gauges.

    Parameters
    ----------
    gauge : str or list[str]
        Single gauge ID or list of gauge IDs.
    """
    reanalysis_path = os.path.join(
        BASE_DIRECTORY, "reanalysis/streamflow.zarr/"
    )
    return open_zarr(reanalysis_path).sel(gauge_id=gauge).compute()


def load_reforecast(gauge=HYBAS_ID):
    reforecast_path = os.path.join(
        BASE_DIRECTORY, "reforecast/streamflow.zarr/"
    )
    return open_zarr(reforecast_path).sel(gauge_id=gauge).compute()


def process_reforecast(ds_rf):
    df_rf = ds_rf.to_dataframe().reset_index()
    df_rf["valid_time"] = df_rf.apply(
        lambda row: row["issue_time"] + row["lead_time"], axis=1
    )
    df_rf["leadtime"] = df_rf["lead_time"].apply(lambda x: x.days)
    df_rf = df_rf.drop(columns=["lead_time", "gauge_id"])
    return df_rf


def process_reanalysis(ds_ra):
    """Convert reanalysis xarray Dataset to a DataFrame.

    Works for both single and multiple gauges. The ``gauge_id`` column is
    preserved so that multi-gauge results can be joined back to LGA data.
    """
    df_ra = ds_ra.to_dataframe().reset_index()
    df_ra = df_ra.rename(columns={"time": "valid_time"})
    return df_ra


def load_return_periods(gauge=HYBAS_ID):
    return_periods_path = os.path.join(BASE_DIRECTORY, "return_periods.zarr/")
    return open_zarr(return_periods_path).sel(gauge_id=gauge).compute()


def get_gauges_by_area(
    area_gdf: gpd.GeoDataFrame,
    include_non_quality_verified: bool = False,
    include_gauges_without_model: bool = False,
) -> gpd.GeoDataFrame:
    """
    Retrieve gauges within an area using the Google Flood Forecasting API.
    Do not cache results for >1 day as gauges are occasionally added/removed.

    Parameters
    ----------
    area_gdf : gpd.GeoDataFrame
        Area of interest (automatically converted to EPSG:4326)
    include_non_quality_verified : bool, default False
        Include gauges with unverified models
    include_gauges_without_model : bool, default False
        Include gauges without hydrological models

    Returns
    -------
    gpd.GeoDataFrame
        Gauges with metadata and geometry (EPSG:4326)
    """
    area_gdf = (
        area_gdf.set_crs("EPSG:4326")
        if area_gdf.crs is None
        else area_gdf.to_crs("EPSG:4326")
    )
    min_lon, min_lat, max_lon, max_lat = area_gdf.total_bounds

    loop = {
        "vertices": [
            {"latitude": min_lat, "longitude": min_lon},
            {"latitude": min_lat, "longitude": max_lon},
            {"latitude": max_lat, "longitude": max_lon},
            {"latitude": max_lat, "longitude": min_lon},
        ]
    }

    request_body = {
        "loop": loop,
        "pageSize": 50000,
        "includeNonQualityVerified": include_non_quality_verified,
        "includeGaugesWithoutHydroModel": include_gauges_without_model,
    }

    all_gauges = []
    while True:
        response = requests.post(
            "https://floodforecasting.googleapis.com/v1/gauges:searchGaugesByArea",  # noqa
            json=request_body,
            params={"key": GOOGLE_API_KEY},
        )
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            raise ValueError(f"API error: {data['error']}")

        all_gauges.extend(data.get("gauges", []))
        if not (next_token := data.get("nextPageToken")):
            break
        request_body["pageToken"] = next_token

    if not all_gauges:
        return gpd.GeoDataFrame(
            columns=[
                "gauge_id",
                "latitude",
                "longitude",
                "site_name",
                "river",
                "source",
                "country_code",
                "quality_verified",
                "has_model",
                "geometry",
            ],
            crs="EPSG:4326",
        )

    records = [
        {
            "gauge_id": g.get("gaugeId"),
            "latitude": g.get("location", {}).get("latitude"),
            "longitude": g.get("location", {}).get("longitude"),
            "site_name": g.get("siteName"),
            "river": g.get("river"),
            "source": g.get("source"),
            "country_code": g.get("countryCode"),
            "quality_verified": g.get("qualityVerified"),
            "has_model": g.get("hasModel"),
        }
        for g in all_gauges
    ]

    gdf = gpd.GeoDataFrame(
        pd.DataFrame(records),
        geometry=gpd.points_from_xy(
            [r["longitude"] for r in records], [r["latitude"] for r in records]
        ),
        crs="EPSG:4326",
    )

    # Filter to gauges within the area (in case some are just outside the bbox)
    gdf = gpd.sjoin(gdf, area_gdf, how="inner", predicate="within")
    gdf = gdf[
        [
            c
            for c in [
                "gauge_id",
                "latitude",
                "longitude",
                "site_name",
                "river",
                "source",
                "country_code",
                "quality_verified",
                "has_model",
                "geometry",
            ]
            if c in gdf.columns
        ]
    ]
    gdf = gdf.drop_duplicates(subset=["gauge_id"])

    return gdf


def get_gauge_models(
    gauge_ids: list[str], batch_size: int = 50
) -> pd.DataFrame:
    """
    Retrieve hydrological model metadata for gauges.

    Parameters
    ----------
    gauge_ids : list[str]
        Gauge IDs to fetch model metadata for
    batch_size : int, default 50
        Gauges per request (max 20,000)

    Returns
    -------
    pd.DataFrame
        Model metadata with thresholds

    Notes
    -----
    Model IDs may change over time. For METERS: thresholds are from local
    authorities. For CUBIC_METERS_PER_SECOND: 2, 5, 20 year return periods.
    """

    all_models = []
    for i in range(0, len(gauge_ids), batch_size):
        batch = gauge_ids[i : i + batch_size]
        names_params = "&".join([f"names=gaugeModels/{gid}" for gid in batch])
        url = f"https://floodforecasting.googleapis.com/v1/gaugeModels:batchGet?key={GOOGLE_API_KEY}&{names_params}"  # noqa

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            raise ValueError(f"API error: {data['error']}")

        all_models.extend(data.get("gaugeModels", []))

    if not all_models:
        return pd.DataFrame(
            columns=[
                "gauge_id",
                "gauge_model_id",
                "gauge_value_unit",
                "quality_verified",
                "warning_level",
                "danger_level",
                "extreme_danger_level",
            ]
        )

    records = [
        {
            "gauge_id": m.get("gaugeId"),
            "gauge_model_id": m.get("gaugeModelId"),
            "gauge_value_unit": m.get("gaugeValueUnit"),
            "quality_verified": m.get("qualityVerified"),
            "warning_level": m.get("thresholds", {}).get("warningLevel"),
            "danger_level": m.get("thresholds", {}).get("dangerLevel"),
            "extreme_danger_level": m.get("thresholds", {}).get(
                "extremeDangerLevel"
            ),
        }
        for m in all_models
    ]

    return pd.DataFrame(records)


def get_gauge_forecasts(
    gauge_ids: list[str],
    issued_time_start: Optional[str] = None,
    issued_time_end: Optional[str] = None,
    batch_size: int = 500,
) -> pd.DataFrame:
    """
    Retrieve recent forecasts for gauges (Oct 2023+).

    For historical data (1980-2023), use load_reanalysis()
    or load_reforecast().

    Parameters
    ----------
    gauge_ids : list[str]
        Gauge IDs to fetch forecasts for
    issued_time_start : str, optional
        Start date in ISO 8601 format (e.g., "2024-01-15"),
        defaults to 7 days ago
    issued_time_end : str, optional
        End date in ISO 8601 format, defaults to now
    batch_size : int, default 500
        Gauges per request (max 500)

    Returns
    -------
    pd.DataFrame
        Forecast data with datetime columns

    Examples
    --------
    >>> forecasts = get_gauge_forecasts(['hybas_1120842550'])
    """
    if not GOOGLE_API_KEY:
        raise ValueError(
            "GOOGLE_API_KEY not found. Please set it in your .env file."
        )

    if not gauge_ids:
        return pd.DataFrame(
            columns=[
                "gauge_id",
                "issued_time",
                "forecast_start",
                "forecast_end",
                "value",
            ]
        )

    # Validate batch size
    if batch_size > 500:
        raise ValueError("batch_size cannot exceed 500")

    # Set default time range if not provided
    if issued_time_start is None:
        issued_time_start = (datetime.now() - timedelta(days=7)).strftime(
            "%Y-%m-%d"
        )
    if issued_time_end is None:
        issued_time_end = datetime.now().strftime("%Y-%m-%d")

    # API endpoint
    url = (
        "https://floodforecasting.googleapis.com/v1/gauges:queryGaugeForecasts"
    )

    # Split gauge IDs into batches
    gauge_id_batches = [
        gauge_ids[i : i + batch_size]
        for i in range(0, len(gauge_ids), batch_size)
    ]

    all_forecasts = []

    # Fetch forecasts in batches
    for batch in gauge_id_batches:
        # Build query parameters
        params = {
            "key": GOOGLE_API_KEY,
            "gaugeIds": batch,
            "issuedTimeStart": issued_time_start,
            "issuedTimeEnd": issued_time_end,
        }

        # Make API request
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        # Check for API errors
        if "error" in data:
            raise ValueError(f"API error: {data['error']}")

        # Extract forecasts from response
        # Response format:
        # {"forecasts": {"gauge_id": {"forecasts": [...]}, ...}}
        forecasts_map = data.get("forecasts", {})

        # Parse nested forecast structure
        for gauge_id, forecast_set in forecasts_map.items():
            forecast_list = forecast_set.get("forecasts", [])

            for forecast in forecast_list:
                issued_time = forecast.get("issuedTime")
                forecast_ranges = forecast.get("forecastRanges", [])

                for forecast_range in forecast_ranges:
                    all_forecasts.append(
                        {
                            "gauge_id": gauge_id,
                            "issued_time": issued_time,
                            "forecast_start": forecast_range.get(
                                "forecastStartTime"
                            ),
                            "forecast_end": forecast_range.get(
                                "forecastEndTime"
                            ),
                            "value": forecast_range.get("value"),
                        }
                    )

    # Convert to DataFrame
    if not all_forecasts:
        return pd.DataFrame(
            columns=[
                "gauge_id",
                "issued_time",
                "forecast_start",
                "forecast_end",
                "value",
            ]
        )

    df = pd.DataFrame(all_forecasts)

    # Convert time columns to datetime
    df["issued_time"] = pd.to_datetime(df["issued_time"])
    df["forecast_start"] = pd.to_datetime(df["forecast_start"])
    df["forecast_end"] = pd.to_datetime(df["forecast_end"])

    return df


def get_latest_forecast(
    gauge_id: str,
    forecast_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get the most recent forecast issued for a gauge on a specific date.

    This is a convenience function that queries forecasts for a single day and
    returns only the latest one. Useful for operational monitoring.

    Args:
        gauge_id: Gauge ID to fetch forecast for
        forecast_date: Date to query forecasts for (YYYY-MM-DD format).
                      Defaults to today.

    Returns:
        pd.DataFrame with columns:
            - gauge_id: Gauge identifier
            - issued_time: When the forecast was generated (latest for the day)
            - forecast_start: Start of forecast period
            - forecast_end: End of forecast period
            - value: Forecast value (units from gauge model)

    Example:
        # Get latest forecast issued today
        latest = get_latest_forecast('hybas_1120842550')

        # Get latest forecast from specific date
        latest = get_latest_forecast('hybas_1120842550', '2024-01-15')
    """
    # Set default date to today if not provided
    if forecast_date is None:
        forecast_date = datetime.now().strftime("%Y-%m-%d")

    # Parse the date to create the end range (next day)
    date_obj = datetime.strptime(forecast_date, "%Y-%m-%d")
    next_day = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    # Get all forecasts for the day
    forecasts = get_gauge_forecasts(
        gauge_ids=[gauge_id],
        issued_time_start=forecast_date,
        issued_time_end=next_day,
    )

    if len(forecasts) == 0:
        return pd.DataFrame(
            columns=[
                "gauge_id",
                "issued_time",
                "forecast_start",
                "forecast_end",
                "value",
            ]
        )

    # Keep only the latest forecast issued on that day
    latest_issue_time = forecasts["issued_time"].max()
    latest_forecast = forecasts[
        forecasts["issued_time"] == latest_issue_time
    ].copy()

    return latest_forecast
