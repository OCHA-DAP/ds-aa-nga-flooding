from src.monitoring import etl, plot

if __name__ == "__main__":
    monitoring_date = etl.resolve_monitoring_date()
    print(f"Checking flood forecast for date: {monitoring_date}")

    df = etl.get_database_forecast(monitoring_date)
    plot.combined_plots(df, save_output=True)
