import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

from src.monitoring import plot

load_dotenv()

GLOFAS_THRESH = 3132
GOOGLE_THRESH = 1195


if __name__ == "__main__":
    engine = stratus.get_engine(stage="dev")
    with engine.connect() as con:
        df = pd.read_sql(
            """
            select * from monitoring.nga_cerf_flooding
            where updated = (select max(updated) from
            monitoring.nga_cerf_flooding)
            """,
            con=con,
        )

    plot.combined_plots(df, GLOFAS_THRESH, GOOGLE_THRESH, save_output=True)
