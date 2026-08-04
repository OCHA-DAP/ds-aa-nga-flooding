import os
from datetime import datetime, timedelta

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

from src.constants import ACTION_GAUGE_THRESHOLDS
from src.datasources import glofas
from src.monitoring import etl

load_dotenv()

if __name__ == "__main__":
    update_date_formatted = os.getenv("MONITORING_DATE", "")
    if not update_date_formatted:
        update_date_formatted = datetime.today().strftime("%Y-%m-%d")
    update_date = datetime.strptime(update_date_formatted, "%Y-%m-%d")

    print(f"Retrieving flood forecasts for date: {update_date_formatted}")
    station_name = "wuroboki"
    overwrite = True

    # --- 1. GloFAS forecast + reanalysis at Wuroboki (readiness trigger)...
    coords = glofas.get_coords(station_name)
    forecast_blob_name = etl.get_blob_name(
        "forecast", station_name, update_date
    )
    etl.get_glofas_forecast(
        forecast_blob_name, coords, update_date, overwrite=overwrite
    )
    df_forecast = etl.process_glofas(
        forecast_blob_name, "glofas_forecast", station_name
    )

    # NOTE that we're saving the reanalysis data based on the day it was
    # MONITORED and NOT for the day that it is valid! The intermediate
    # reanalysis lags by a few days (more during CDS product transitions),
    # so walk back from -2 days until an available day is found. A missing
    # reanalysis must never break the run — the readiness forecast branch
    # and the action trigger still work without it.
    reanalysis_blob_name = etl.get_blob_name(
        "reanalysis", station_name, update_date
    )
    df_reanalysis = pd.DataFrame()
    for days_back in range(2, 8):
        reanalysis_update = update_date - timedelta(days=days_back)
        try:
            etl.get_glofas_reanalysis(
                reanalysis_blob_name,
                coords,
                reanalysis_update,
                overwrite=overwrite,
            )
            df_reanalysis = etl.process_glofas(
                reanalysis_blob_name, "glofas_reanalysis", station_name
            )
            break
        except Exception as e:
            print(f"No reanalysis for {reanalysis_update.date()}: {e}")
    if df_reanalysis.empty:
        print("WARNING: no GloFAS reanalysis available in the last 7 days")

    # --- 2. Google forecasts for the action-trigger gauges...
    df_google = etl.get_google_forecasts(
        list(ACTION_GAUGE_THRESHOLDS), update_date
    )

    # --- 3. Combine and save to database...
    df_all = pd.concat([df_forecast, df_reanalysis, df_google])
    df_all["monitoring_date"] = update_date
    engine = stratus.get_engine(stage="dev", write=True)
    df_all.to_sql(
        etl.DB_TABLE,  # This table was created manually
        schema=etl.DB_SCHEMA,
        con=engine,
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
    )
    print(f"{len(df_all)} rows saved to {etl.DB_SCHEMA}.{etl.DB_TABLE}!")
