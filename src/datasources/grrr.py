import os

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
    df_ra = ds_ra.to_dataframe().reset_index()
    df_ra = df_ra.rename(columns={"time": "valid_time"}).drop(
        columns=["gauge_id"]
    )
    return df_ra


def load_return_periods(gauge=HYBAS_ID):
    return_periods_path = os.path.join(BASE_DIRECTORY, "return_periods.zarr/")
    return open_zarr(return_periods_path).sel(gauge_id=gauge).compute()
