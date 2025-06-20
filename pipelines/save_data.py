from datetime import datetime, timedelta

import ocha_stratus as stratus
import pandas as pd

from src.datasources import glofas, grrr
from src.monitoring import etl, input

if __name__ == "__main__":
    args = input.parse_args()
    update_date_formatted = args.date
    update_date = datetime.strptime(args.date, "%Y-%m-%d")

    print(f"Retrieving flood forecast for date: {args.date}")
    reanalysis_update = update_date - timedelta(days=2)
    station_name = "wuroboki"
    overwrite = False

    coords = glofas.get_coords(station_name)
    forecast_blob_name = etl.get_blob_name(
        "forecast", station_name, update_date
    )
    # NOTE that we're saving the reanalysis data based on the day it was
    # MONITORED and NOT for the day that it is valid!
    reanalysis_blob_name = etl.get_blob_name(
        "reanalysis", station_name, update_date
    )

    # --- 1. Saving raw GloFAS data...
    etl.get_glofas_forecast(
        forecast_blob_name, coords, update_date, overwrite=overwrite
    )
    etl.get_glofas_reanalysis(
        reanalysis_blob_name, coords, reanalysis_update, overwrite=overwrite
    )

    # --- 2. Get the Glofas dataframes...
    df_forecast = etl.process_glofas(
        forecast_blob_name, "glofas_forecast", station_name
    )
    df_reanalysis = etl.process_glofas(
        reanalysis_blob_name, "glofas_reanalysis", station_name
    )

    # --- 3. Getting Google dataframe...
    df_google = etl.get_google_forecast(grrr.HYBAS_ID, update_date)

    # --- 4. Combine and save to database...
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
