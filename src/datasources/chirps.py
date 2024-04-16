import calendar
import datetime
import os
from pathlib import Path

import cftime
import pandas as pd
import requests
import xarray as xr
from tqdm import tqdm

from src.datasources import codab

DATA_DIR = Path(os.environ["AA_DATA_DIR_NEW"])
CHIRPS_RAW_DIR = DATA_DIR / "public" / "raw" / "nga" / "chirps" / "daily"
CHIRPS_PROC_DIR = DATA_DIR / "public" / "processed" / "nga" / "chirps"
CHIRPS_BASE_URL = (
    "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
)


def load_raster_stats():
    return pd.read_csv(
        CHIRPS_PROC_DIR / "chirps-daily-stats.csv", parse_dates=["T"]
    )


def calculate_raster_stats():
    ds = load_chirps_daily()
    adm = codab.load_codab(admin_level=2, aoi_only=True)

    dfs = []
    for pcode, group in tqdm(adm.groupby("ADM2_PCODE")):
        ds_clip = ds.rio.clip(group.geometry, all_touched=True)
        stats = ds_clip.mean(dim=["X", "Y"]).rename({"prcp": "mean"})

        # for q in range(10, 91, 10):
        #     stats[f"q{q}"] = ds_clip["prcp"].quantile(q / 100,
        #     dim=["X", "Y"])
        # stats = stats.drop_vars("quantile")
        df_in = (
            stats.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        df_in["ADM2_PCODE"] = pcode
        dfs.append(df_in)
    df = pd.concat(dfs, ignore_index=True)

    df["T"] = df["T"].dt.date
    filename = "chirps-daily-stats.csv"
    df.to_csv(CHIRPS_PROC_DIR / filename, index=False)


def load_chirps_daily():
    ds = xr.open_dataset(CHIRPS_PROC_DIR / "chirps-daily-1998-2023.nc")
    ds = ds.rio.write_crs(4326)
    return ds


def process_chirps_daily():
    if not CHIRPS_PROC_DIR.exists():
        os.makedirs(CHIRPS_PROC_DIR, exist_ok=True)
    dates = pd.date_range(start="1998-01-01", end="2023-12-31", freq="D")

    ds_ins = []
    for date in tqdm(dates):
        filename = f"chirps-daily-{date.date()}.nc"
        try:
            ds_in = xr.load_dataset(CHIRPS_RAW_DIR / filename)
            ds_ins.append(ds_in)
        except Exception as e:
            print(f"Failed to load {date.date()}")
            print(e)
            pass

    ds_concat = xr.concat(ds_ins, dim="T")
    ds_concat = ds_concat.assign_coords(
        T=cftime.datetime.fromordinal(
            ds_concat.T.values, calendar="standard", has_year_zero=False
        )
    )
    # ds_concat.T.attrs["calendar"] = "360_day"
    # ds_concat = xr.decode_cf(ds_concat)
    filename = "chirps-daily-1998-2023.nc"
    ds_concat.to_netcdf(CHIRPS_PROC_DIR / filename)


def download_chirps_daily(
    d: datetime.datetime, total_bounds, clobber: bool = False
):
    if not CHIRPS_RAW_DIR.exists():
        os.makedirs(CHIRPS_RAW_DIR, exist_ok=True)

    lon_min, lat_min, lon_max, lat_max = total_bounds
    location_url = (
        f"X/%28{lon_min}%29%28{lon_max}"
        f"%29RANGEEDGES/"
        f"Y/%28{lat_max}%29%28{lat_min}"
        f"%29RANGEEDGES/"
    )

    resolution = 0.05

    year = str(d.year)
    month = f"{d.month:02d}"
    day = f"{d.day:02d}"

    month_name = calendar.month_abbr[int(month)]

    filepath = CHIRPS_RAW_DIR / f"chirps-daily-{year}-{month}-{day}.nc"
    if filepath.exists() and not clobber:
        return

    url = (
        f"{CHIRPS_BASE_URL}"
        ".daily-improved/.global/."
        f"{str(resolution).replace('.', 'p')}/.prcp/"
        f"{location_url}"
        f"T/%28{day}%20{month_name}%20{year}%29%28{day}"
        f"%20{month_name}%20{year}"
        "%29RANGEEDGES/data.nc"
    )
    try:
        response = requests.get(url)
        with open(filepath, "wb") as out_file:
            out_file.write(response.content)
    except Exception as e:
        print(f"Failed to download {d.date()}")
        print(e)
        return
