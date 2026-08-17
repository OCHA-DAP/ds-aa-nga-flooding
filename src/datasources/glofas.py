import os
from pathlib import Path
from typing import Literal

import cdsapi
import numpy as np
import ocha_stratus as stratus
import pandas as pd
import xarray as xr
from dotenv import load_dotenv
from tqdm.auto import tqdm

import src.constants
from src.constants import (
    WUROBOKI_2YRPR,
    WUROBOKI_3YRPR,
    WUROBOKI_5YRPR,
    WUROBOKI_LAT,
    WUROBOKI_LON,
)
from src.utils import blob, cds_utils

load_dotenv()

DATA_DIR = Path(os.getenv("AA_DATA_DIR", "."))
GF_RAW_DIR = (
    DATA_DIR / "public" / "raw" / "nga" / "glofas" / "cems-glofas-historical"
)
GF_REFORECAST_RAW_DIR = (
    DATA_DIR / "public" / "raw" / "nga" / "glofas" / "cems-glofas-reforecast"
)
GF_TEST_DIR = DATA_DIR / "public" / "raw" / "nga" / "glofas" / "test"
GF_PROC_DIR = DATA_DIR / "public" / "processed" / "nga" / "glofas"

GF_STATIONS = {
    "wuroboki": {"lon": 12.767, "lat": 9.383},
    "ibi": {"lon": 9.725, "lat": 8.175},
    "makurdi": {"lon": 8.525, "lat": 7.775},
    "umaisha": {"lon": 7.175, "lat": 7.975},
    "lokoja": {"lon": 6.775, "lat": 7.775},
    "baro": {"lon": 6.425, "lat": 8.575},
    "wuya": {"lon": 5.825, "lat": 9.075},
    "jebba": {"lon": 4.875, "lat": 9.175},
    "onitsha": {"lon": 6.725, "lat": 6.225},
    "yidere bode": {"lon": 4.125, "lat": 11.375},
    "kende": {"lon": 4.225, "lat": 11.475},
}

RAINY_SEASON_MONTHS = [
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]
ALL_MONTHS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]


def get_coords(station_name):
    station = GF_STATIONS[station_name]
    glofas_lon, glofas_lat = get_glofas_grid_coords(
        station["lon"], station["lat"]
    )
    pitch = 0.001
    N = glofas_lat + pitch
    S = glofas_lat
    E = glofas_lon + pitch
    W = glofas_lon
    return [N, W, S, E]


def get_blob_name(
    data_type: Literal["raw", "processed"],
    dataset: Literal["reanalysis", "reforecast", "forecast"],
    station_name: str,
    year: int = None,
) -> str:
    if year is None and data_type == "raw":
        raise ValueError("Year must be provided for raw data")
    if data_type == "raw":
        return f"{src.constants.PROJECT_PREFIX}/{data_type}/glofas/{dataset}/glofas_{data_type}_{dataset}_{station_name}_{year}.grib"  # noqa
    return f"{src.constants.PROJECT_PREFIX}/{data_type}/glofas/glofas_{dataset}_{station_name}.parquet"  # noqa


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
                        "hday": [f"{x:02}" for x in range(1, 32)],  # noqa
                        "leadtime_hour": [str(x) for x in leadtime_chunk],
                        "format": "grib",
                        "area": [N, W, S, E],
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
                    "hday": [f"{x:02}" for x in range(1, 32)],  # noqa
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
                    "area": [N, W, S, E],
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


def get_glofas_grid_coords(lon, lat):
    grid_lat = np.arange(-90.025, 90, 0.05)
    grid_lon = np.arange(-180.025, 180, 0.05)
    nearest_lat_idx = (np.abs(grid_lat - lat)).argmin()
    nearest_lon_idx = (np.abs(grid_lon - lon)).argmin()
    return round(grid_lon[nearest_lon_idx], 3), round(
        grid_lat[nearest_lat_idx], 3
    )


def download_glofas_reanalysis_year_to_blob(
    year: int, station_name: str, pitch: float = 0.001, clobber: bool = False
):
    station = GF_STATIONS[station_name]
    glofas_lon, glofas_lat = get_glofas_grid_coords(
        station["lon"], station["lat"]
    )
    N = glofas_lat + pitch
    S = glofas_lat
    E = glofas_lon + pitch
    W = glofas_lon
    dataset = "cems-glofas-historical"
    request = {
        "system_version": ["version_4_0"],
        "hydrological_model": ["lisflood"],
        "product_type": ["consolidated"],
        "variable": ["river_discharge_in_the_last_24_hours"],
        "hyear": [f"{year}"],
        "hmonth": [f"{x:02}" for x in range(1, 13)],  # noqa
        "hday": [f"{x:02}" for x in range(1, 32)],  # noqa
        "data_format": "grib2",
        "download_format": "unarchived",
        "area": [N, W, S, E],
    }
    blob_name = get_blob_name("raw", "reanalysis", station_name, year)
    if not clobber and blob.check_blob_exists(blob_name):
        print(f"{blob_name} already exists in blob storage")
        return
    return cds_utils.download_raw_cds_api_to_blob(dataset, request, blob_name)


def load_glofas_reanalysis_year(
    data_type: Literal["raw", "processed"], station_name: str, year: int
):
    blob_name = get_blob_name(data_type, "reanalysis", station_name, year)
    if data_type == "raw":
        local_filepath = "temp" / Path(blob_name)
        if local_filepath.exists():
            return xr.load_dataset(local_filepath)
        else:
            blob_data = blob.load_blob_data(blob_name)
            print(f"Downloading {blob_name} to {local_filepath}")
            if not local_filepath.parent.exists():
                os.makedirs(local_filepath.parent)
            with open(local_filepath, "wb") as file:
                file.write(blob_data)
            return xr.load_dataset(local_filepath)
    elif data_type == "processed":
        return blob.load_parquet_from_blob(blob_name)


def process_glofas_reanalysis(station_name: str):
    sample_blob = get_blob_name("raw", "reanalysis", station_name, year=0)
    raw_blob_prefix = sample_blob[: -len("0.grib")]
    blob_names = [
        x
        for x in blob.list_container_blobs(name_starts_with=raw_blob_prefix)
        if x.endswith(".grib")
    ]
    dfs = []
    for blob_name in tqdm(blob_names):
        year = int(blob_name.split(".")[0].split("_")[-1])
        ds = load_glofas_reanalysis_year("raw", station_name, year)
        df_in = ds["dis24"].to_dataframe().reset_index()[["time", "dis24"]]
        dfs.append(df_in)
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values("time")
    blob_name = get_blob_name("processed", "reanalysis", station_name)
    stratus.upload_parquet_to_blob(df, blob_name)


def download_glofas_reanalysis_to_blob(station_name: str):
    for year in tqdm(range(1979, 2025)):
        download_glofas_reanalysis_year_to_blob(year, station_name)


def download_reforecast_all_stations_year(
    year: int,
    product_type: Literal["ensemble", "control"],
    max_leadtime_days: int = 46,
    max_leadtime_chunk: int = 7,
    rainy_season_only: bool = False,
    clobber: bool = False,
):
    """Download reforecast data for all GF_STATIONS for a single year.

    Uses a single bounding box covering all stations. Requests are split by
    leadtime chunk to keep CDS API payloads small. Files are saved to blob
    storage as grib2.
    """
    lons = [s["lon"] for s in GF_STATIONS.values()]
    lats = [s["lat"] for s in GF_STATIONS.values()]
    pitch = 0.001
    N = round(max(lats) + pitch, 3)
    S = round(min(lats), 3)
    E = round(max(lons) + pitch, 3)
    W = round(min(lons), 3)

    product_type_str = (
        "ensemble_perturbed_reforecasts"
        if product_type == "ensemble"
        else "control_reforecast"
    )
    months = RAINY_SEASON_MONTHS if rainy_season_only else ALL_MONTHS

    leadtimes = [x * 24 for x in range(1, max_leadtime_days + 1)]
    leadtime_chunks = [
        leadtimes[i : i + max_leadtime_chunk]
        for i in range(0, len(leadtimes), max_leadtime_chunk)
    ]

    dataset = "cems-glofas-reforecast"

    for lt_chunk in tqdm(leadtime_chunks, desc=f"leadtime chunks ({year})"):
        lt_str = f"{lt_chunk[0]}-{lt_chunk[-1]}"
        blob_name = (
            f"{src.constants.PROJECT_PREFIX}/raw/glofas/reforecast_all/"
            f"glofas_raw_reforecast_all_{product_type}_{year}_lt{lt_str}.grib"
        )
        if not clobber and blob.check_blob_exists(blob_name):
            print(f"{blob_name} already exists, skipping")
            continue
        request = {
            "system_version": ["version_4_0"],
            "hydrological_model": ["lisflood"],
            "product_type": [product_type_str],
            "variable": ["river_discharge_in_the_last_24_hours"],
            "hyear": [f"{year}"],
            "hmonth": months,
            "hday": [f"{x:02}" for x in range(1, 32)],  # noqa
            "leadtime_hour": [str(x) for x in lt_chunk],
            "data_format": "grib2",
            "download_format": "unarchived",
            "area": [N, W, S, E],
        }
        try:
            cds_utils.download_raw_cds_api_to_blob(dataset, request, blob_name)
        except Exception as e:
            print(f"Failed for year={year} lt={lt_str}: {e}")


def download_reforecast_all_stations(
    product_type: Literal["ensemble", "control"],
    years: range = range(2003, 2025),
    max_leadtime_days: int = 46,
    max_leadtime_chunk: int = 7,
    rainy_season_only: bool = False,
    clobber: bool = False,
):
    """Download reforecast data for all GF_STATIONS across multiple years."""
    for year in tqdm(years, desc="years"):
        download_reforecast_all_stations_year(
            year=year,
            product_type=product_type,
            max_leadtime_days=max_leadtime_days,
            max_leadtime_chunk=max_leadtime_chunk,
            rainy_season_only=rainy_season_only,
            clobber=clobber,
        )


def load_glofas_reanalysis(station_name: str):
    blob_name = get_blob_name("processed", "reanalysis", station_name)
    return blob.load_parquet_from_blob(blob_name)


def process_glofas_reforecast(
    station_name: str,
    product_type: Literal["ensemble", "control"] = "ensemble",
):
    """Process all raw reforecast GRIBs for a station into a single parquet.

    Blob pattern (from download_glofas_reforecast.ipynb):
      raw/glofas/reforecast/glofas_raw_reforecast_{station}_{product}_{year}_lt{lt}.grib
    """
    raw_prefix = (
        f"{src.constants.PROJECT_PREFIX}/raw/glofas/reforecast/"
        f"glofas_raw_reforecast_{station_name}_{product_type}_"
    )
    blob_names = sorted(
        x
        for x in blob.list_container_blobs(name_starts_with=raw_prefix)
        if x.endswith(".grib")
    )
    station = GF_STATIONS[station_name]
    glofas_lon, glofas_lat = get_glofas_grid_coords(
        station["lon"], station["lat"]
    )

    dfs = []
    for blob_name in tqdm(blob_names):
        local_path = Path("temp") / Path(blob_name)
        if not local_path.exists():
            blob_data = blob.load_blob_data(blob_name)
            if not local_path.parent.exists():
                os.makedirs(local_path.parent)
            with open(local_path, "wb") as f:
                f.write(blob_data)
        try:
            ds = xr.open_dataset(
                local_path, engine="cfgrib", backend_kwargs={"indexpath": ""}
            )
        except Exception as e:
            print(f"Warning: skipping {blob_name}: {e}")
            continue
        da = ds["dis24"].sel(
            latitude=glofas_lat, longitude=glofas_lon, method="nearest"
        )
        df_in = da.to_dataframe().reset_index()
        keep = [
            c
            for c in ["number", "time", "step", "valid_time", "dis24"]
            if c in df_in.columns
        ]
        df_in = df_in[keep]
        df_in["leadtime"] = df_in["step"].dt.days
        df_in = df_in.drop(columns=["step"])
        dfs.append(df_in)

    df = pd.concat(dfs, ignore_index=True)
    sort_cols = [c for c in ["time", "leadtime", "number"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)
    out_blob = (
        f"{src.constants.PROJECT_PREFIX}/processed/glofas/"
        f"glofas_reforecast_{station_name}_{product_type}.parquet"
    )
    stratus.upload_parquet_to_blob(df, out_blob)


def load_glofas_reforecast(
    station_name: str,
    product_type: Literal["ensemble", "control"] = "ensemble",
):
    station_cfg = next(
        (
            v
            for v in src.constants.STATE_CONFIG.values()
            if v["glofas_station"] == station_name
        ),
        None,
    )
    if station_cfg and station_cfg.get("glofas_reforecast_blob"):
        blob_name = station_cfg["glofas_reforecast_blob"]
    else:
        blob_name = (
            f"{src.constants.PROJECT_PREFIX}/processed/glofas/"
            f"glofas_reforecast_{station_name}_{product_type}.parquet"
        )
    return stratus.load_parquet_from_blob(blob_name)
