import os
from pathlib import Path

import numpy as np
import ocha_stratus as stratus
import pandas as pd
import requests
import rioxarray as rxr

from src.constants import ISO3, PROJECT_PREFIX
from src.datasources import codab

AA_DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW", "."))
RAW_WP_PATH = (
    AA_DATA_DIR
    / "public"
    / "raw"
    / "nga"
    / "worldpop"
    / "nga_ppp_2020_1km_Aggregated_UNadj.tif"
)
PROC_WP_DIR = AA_DATA_DIR / "public" / "processed" / "nga" / "worldpop"
WORLDPOP_BASE_URL = (
    "https://data.worldpop.org/GIS/Population/"
    "Global_2000_2020_1km_UNadj/2020/{iso3_upper}/"
    "{iso3}_ppp_2020_1km_Aggregated_UNadj.tif"
)


def load_raw_worldpop():
    da = rxr.open_rasterio(RAW_WP_PATH)
    return da.where(da != da.attrs["_FillValue"])


def aggregate_worldpop_to_adm2():
    pop = load_raw_worldpop()
    adm2 = codab.load_codab(admin_level=2)
    dicts = []
    for _, row in adm2.iterrows():
        da_clip = pop.rio.clip([row.geometry])
        da_clip = da_clip.where(da_clip > 0)
        dicts.append(
            {
                "total_pop": da_clip.sum().values,
                "ADM2_PCODE": row["ADM2_PCODE"],
            }
        )
    df_pop = pd.DataFrame(dicts)
    filename = "nga_adm2_2020_1km_Aggregated_UNadj.csv"
    df_pop.to_csv(PROC_WP_DIR / filename, index=False)


def load_adm2_worldpop():
    filename = "nga_adm2_2020_1km_Aggregated_UNadj.csv"
    return pd.read_csv(PROC_WP_DIR / filename)


def get_blob_name(iso3: str):
    iso3 = iso3.lower()
    return (
        f"{PROJECT_PREFIX}/raw/worldpop/"
        f"{iso3}_ppp_2020_1km_Aggregated_UNadj.tif"
    )


def download_worldpop_to_blob(iso3: str = ISO3, clobber: bool = False):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    if not clobber and blob_name in stratus.list_container_blobs(
        name_starts_with=f"{PROJECT_PREFIX}/raw/worldpop/", stage="dev"
    ):
        print(f"{blob_name} already exists in blob storage")
        return
    url = WORLDPOP_BASE_URL.format(iso3_upper=iso3.upper(), iso3=iso3)
    response = requests.get(url)
    response.raise_for_status()
    stratus.upload_blob_data(response.content, blob_name, stage="dev")


def load_worldpop_from_blob(iso3: str = ISO3):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    da = stratus.open_blob_cog(blob_name, stage="dev")
    da = da.where(da != da.attrs["_FillValue"]).squeeze(drop=True)
    da.attrs["_FillValue"] = np.nan
    return da
