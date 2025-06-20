from src.constants import GLOFAS_THRESH, GOOGLE_THRESH
from src.monitoring import etl, input, plot

if __name__ == "__main__":
    args = input.parse_args()
    update_date = args.date
    print(f"Checking flood forecast for date: {update_date}")

    df = etl.get_forecast()
    plot.combined_plots(df, GLOFAS_THRESH, GOOGLE_THRESH, save_output=True)
