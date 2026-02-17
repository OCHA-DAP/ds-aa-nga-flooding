import ocha_stratus as stratus
import pandas as pd

import src.constants


def load_wuroboki():
    blob_name = f"{src.constants.PROJECT_PREFIX}/raw/AA-nigeria_data/NiHSA/wuroboki_water level.xls"  # noqa
    df = stratus.load_excel_from_blob(
        blob_name, skiprows=1, parse_dates=["Date"]
    )
    df = df.rename(
        columns={
            "Date": "time",
            "Hauteur écoulement (cm REFERENTIEL HYDROM)": "level",
        }
    ).drop(columns=["Validity"])
    df = df[(df["level"] >= 0) & (df["level"] < 999999)]
    df["time"] = pd.to_datetime(df["time"].dt.date)
    return df


def load_hydological_areas():
    blob_name = (
        f"{src.constants.PROJECT_PREFIX}/raw/AA-nigeria_data/NiHSA/HA.zip"
    )
    gdf = stratus.load_shp_from_blob(blob_name, shapefile="HA.shp")
    return gdf


def load_communities(shp_name="shp/2025HFR_Communities.shp"):
    blob_name = f"{src.constants.PROJECT_PREFIX}/raw/AA-nigeria_data/NiHSA/flood_risk_communities.zip"  # noqa
    gdf = stratus.load_shp_from_blob(blob_name, shp_name)
    return gdf


WUROBOKI_COMPLETE_YEARS = [
    1979,
    1983,
    1988,
    1989,
    1991,
    1992,
    1993,  # maybe
    2006,
    2012,  # maybe
    2016,  # maybe
    2018,  # maybe
    2021,  # but remove June, I think
    2023,
    2024,  # but still weird
]
