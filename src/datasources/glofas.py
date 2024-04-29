import os
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr
from tqdm import tqdm

from src.constants import WUROBOKI_LAT, WUROBOKI_LON

DATA_DIR = Path(os.getenv("AA_DATA_DIR"))
GF_RAW_DIR = (
    DATA_DIR / "public" / "raw" / "nga" / "glofas" / "cems-glofas-historical"
)
GF_REFORECAST_RAW_DIR = (
    DATA_DIR / "public" / "raw" / "nga" / "glofas" / "cems-glofas-reforecast"
)
GF_TEST_DIR = DATA_DIR / "public" / "raw" / "nga" / "glofas" / "test"
GF_PROC_DIR = DATA_DIR / "public" / "processed" / "nga" / "glofas"


def process_reanalysis():
    """Process reanalysis for Wuroboki station only"""
    files = [x for x in os.listdir(GF_RAW_DIR) if x.endswith(".grib")]
    dfs = []
    for file in tqdm(files):
        da_in = xr.load_dataset(GF_RAW_DIR / file, engine="cfgrib")["dis24"]
        df_in = (
            da_in.sel(
                latitude=WUROBOKI_LAT, longitude=WUROBOKI_LON, method="nearest"
            )
            .to_dataframe()
            .reset_index()[["time", "dis24"]]
        )
        dfs.append(df_in)
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values("time")
    filename = "wuroboki_glofas_reanalysis.csv"
    df.to_csv(GF_PROC_DIR / filename, index=False)


def load_reanalysis():
    filename = "wuroboki_glofas_reanalysis.csv"
    return pd.read_csv(GF_PROC_DIR / filename, parse_dates=["time"])


def download_reforecast(clobber: bool = False):
    """Download reforecast data for Wuroboki station only"""
    pitch = 0.005
    N, S, E, W = (
        WUROBOKI_LAT + pitch,
        WUROBOKI_LAT - pitch,
        WUROBOKI_LON + pitch,
        WUROBOKI_LON - pitch,
    )
    c = cdsapi.Client()

    if not GF_REFORECAST_RAW_DIR.exists():
        GF_REFORECAST_RAW_DIR.mkdir(parents=True)

    years = range(2003, 2024)
    for year in years:
        save_path = GF_REFORECAST_RAW_DIR / f"wuroboki_reforecast_{year}.grib"
        if save_path.exists() and not clobber:
            print(f"Skipping {year}, already exists")
            continue
        try:
            c.retrieve(
                "cems-glofas-reforecast",
                {
                    "system_version": "version_4_0",
                    "hydrological_model": "lisflood",
                    "product_type": [
                        "control_reforecast",
                    ],
                    "variable": "river_discharge_in_the_last_24_hours",
                    "hyear": f"{year}",
                    "hmonth": [
                        "august",
                        "july",
                        "october",
                        "september",
                    ],
                    "hday": [
                        "01",
                        "02",
                        "03",
                        "04",
                        "05",
                        "06",
                        "07",
                        "08",
                        "09",
                        "10",
                        "11",
                        "12",
                        "13",
                        "14",
                        "15",
                        "16",
                        "17",
                        "18",
                        "19",
                        "20",
                        "21",
                        "22",
                        "23",
                        "24",
                        "25",
                        "26",
                        "27",
                        "28",
                        "29",
                        "30",
                        "31",
                    ],
                    "leadtime_hour": [
                        "24",
                        "48",
                        "72",
                        "96",
                        "120",
                        "144",
                        "168",
                    ],
                    "format": "grib",
                    "area": [
                        N,
                        W,
                        S,
                        E,
                    ],
                },
                save_path,
            )

        except Exception as e:
            print(f"Failed to download {year}")
            print(e)


def process_reforecast():
    """Process reforecast data for Wuroboki station only"""
    files = [
        x for x in os.listdir(GF_REFORECAST_RAW_DIR) if x.endswith(".grib")
    ]
    dfs = []
    for file in tqdm(files):
        da_in = xr.load_dataset(
            GF_REFORECAST_RAW_DIR / file,
            engine="cfgrib",
            backend_kwargs={
                "indexpath": "",
            },
        )
        df_in = (
            da_in.sel(
                latitude=WUROBOKI_LAT, longitude=WUROBOKI_LON, method="nearest"
            )
            .to_dataframe()[["dis24", "valid_time"]]
            .reset_index()
        )
        df_in["leadtime"] = df_in["step"].dt.days
        df_in = df_in.drop(columns=["step"])
        dfs.append(df_in)
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values("time")
    filename = "wuroboki_glofas_reforecast.csv"
    df.to_csv(GF_PROC_DIR / filename, index=False)


def load_reforecast():
    filename = "wuroboki_glofas_reforecast.csv"
    return pd.read_csv(
        GF_PROC_DIR / filename, parse_dates=["time", "valid_time"]
    )
