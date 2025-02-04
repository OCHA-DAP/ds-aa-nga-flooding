import os
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr
from tqdm import tqdm

from src.constants import (
    WUROBOKI_2YRPR,
    WUROBOKI_3YRPR,
    WUROBOKI_5YRPR,
    WUROBOKI_LAT,
    WUROBOKI_LON,
)

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


def download_reforecast_ensembles():
    pitch = 0.005
    N, S, E, W = (
        WUROBOKI_LAT + pitch,
        WUROBOKI_LAT - pitch,
        WUROBOKI_LON + pitch,
        WUROBOKI_LON - pitch,
    )
    c = cdsapi.Client()

    years = range(2003, 2023)

    leadtimes = [x * 24 for x in range(1, 47)]
    max_leadtime_chunk = 7
    leadtime_chunks = [
        leadtimes[x : x + max_leadtime_chunk]
        for x in range(0, len(leadtimes), max_leadtime_chunk)
    ]

    for leadtime_chunk in tqdm(leadtime_chunks):
        lt_chunk_str = f"{leadtime_chunk[0]}-{leadtime_chunk[-1]}"
        for year in tqdm(years):
            save_path = (
                GF_REFORECAST_RAW_DIR
                / f"wuroboki_reforecast_ens_{year}_lt{lt_chunk_str}.grib"
            )
            if save_path.exists():
                print(f"Skipping {year} {lt_chunk_str}, already exists")
                continue
            try:
                c.retrieve(
                    "cems-glofas-reforecast",
                    {
                        "system_version": "version_4_0",
                        "hydrological_model": "lisflood",
                        "product_type": [
                            "ensemble_perturbed_reforecasts",
                        ],
                        "variable": "river_discharge_in_the_last_24_hours",
                        "hyear": f"{year}",
                        "hmonth": [
                            "august",
                            "july",
                            "october",
                            "september",
                        ],
                        "hday": [f"{x:02}" for x in range(1, 32)],
                        "leadtime_hour": [str(x) for x in leadtime_chunk],
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
                print(f"Failed to download {year} {lt_chunk_str}")
                print(e)


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
                    "hday": [f"{x:02}" for x in range(1, 32)],
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


def process_reforecast_ensembles():
    filenames = [x for x in os.listdir(GF_REFORECAST_RAW_DIR) if "ens" in x]

    dfs = []
    for filename in tqdm(filenames):
        filepath = GF_REFORECAST_RAW_DIR / filename
        ds_in = xr.open_dataset(
            filepath,
            engine="cfgrib",
            backend_kwargs={
                "indexpath": "",
            },
        )
        df_in = (
            ds_in.sel(
                latitude=WUROBOKI_LAT, longitude=WUROBOKI_LON, method="nearest"
            )
            .to_dataframe()[["dis24", "valid_time"]]
            .reset_index()
        )
        df_in["leadtime"] = df_in["step"].dt.days
        df_in = df_in.drop(columns=["step"])
        dfs.append(df_in)

    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values(["time", "leadtime"])
    filename = "wuroboki_glofas_reforecast_ens.parquet"
    df.to_parquet(GF_PROC_DIR / filename)


def process_reforecast_frac():
    df = pd.read_parquet(
        GF_PROC_DIR / "wuroboki_glofas_reforecast_ens.parquet"
    )

    df["2yr_thresh"] = df["dis24"] > WUROBOKI_2YRPR
    df["3yr_thresh"] = df["dis24"] > WUROBOKI_3YRPR
    df["5yr_thresh"] = df["dis24"] > WUROBOKI_5YRPR

    ens = (
        df.groupby(["time", "leadtime", "valid_time"])[
            [x for x in df.columns if "yr_thresh" in x]
        ]
        .mean()
        .reset_index()
    )
    filename = "wuroboki_glofas_reforecast_frac.parquet"
    ens.to_parquet(GF_PROC_DIR / filename)


def load_reforecast_frac():
    filename = "wuroboki_glofas_reforecast_frac.parquet"
    return pd.read_parquet(GF_PROC_DIR / filename)


def load_reforecast():
    filename = "wuroboki_glofas_reforecast.csv"
    return pd.read_csv(
        GF_PROC_DIR / filename, parse_dates=["time", "valid_time"]
    )
