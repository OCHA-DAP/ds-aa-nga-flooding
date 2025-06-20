from src.constants import GLOFAS_THRESH, GOOGLE_THRESH
from src.monitoring import etl, plot

if __name__ == "__main__":
    df = etl.get_latest_forecast()
    plot.combined_plots(df, GLOFAS_THRESH, GOOGLE_THRESH, save_output=True)
