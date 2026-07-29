import os
from datetime import datetime

from src.monitoring import etl, plot

if __name__ == "__main__":
    monitoring_date = os.getenv("MONITORING_DATE", "")
    if not monitoring_date:
        monitoring_date = datetime.today().strftime("%Y-%m-%d")
    monitoring_date = datetime.strptime(monitoring_date, "%Y-%m-%d")
    print(f"Checking flood forecast for date: {monitoring_date}")

    df = etl.get_database_forecast(monitoring_date)
    plot.combined_plots(df, save_output=True)
