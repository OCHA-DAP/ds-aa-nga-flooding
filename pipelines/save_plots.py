import os
from datetime import datetime

from src.constants import GLOFAS_THRESH, GOOGLE_THRESH
from src.monitoring import etl, plot

if __name__ == "__main__":
    monitoring_date = os.getenv(
        "MONITORING_DATE", datetime.today().strftime("%Y-%m-%d")
    )
    print(f"Checking flood forecast for date: {monitoring_date}")

    df = etl.get_database_forecast(monitoring_date)
    plot.combined_plots(df, GLOFAS_THRESH, GOOGLE_THRESH, save_output=True)
