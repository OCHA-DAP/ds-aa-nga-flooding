from src.utils import blob


def load_wuroboki():
    blob_name = f"{blob.PROJECT_PREFIX}/raw/AA-nigeria_data/NiHSA/wuroboki_water level.xls"  # noqa
    df = blob.load_excel_from_blob(blob_name, skiprows=1, parse_dates=["Date"])
    df = df.rename(
        columns={
            "Date": "time",
            "Hauteur écoulement (cm REFERENTIEL HYDROM)": "level",
        }
    ).drop(columns=["Validity"])
    df = df[(df["level"] >= 0) & (df["level"] < 999999)]
    return df
