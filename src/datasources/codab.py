import os
from pathlib import Path

import geopandas as gpd
import ocha_stratus as stratus
import requests
from dotenv import load_dotenv

import src.constants
from src.constants import AOI_ADM1_PCODES
from src.utils import blob

load_dotenv()

DATA_DIR = Path(os.getenv("AA_DATA_DIR", "."))
CODAB_DIR = DATA_DIR / "public" / "raw" / "nga" / "cod_ab"


def load_codab(admin_level: int = 0, aoi_only: bool = False):
    filename = f"nga_admbnda_adm{admin_level}_osgof_20190417.shp"
    gdf = gpd.read_file(CODAB_DIR / filename)
    if aoi_only:
        gdf = gdf[gdf["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
    return gdf


def download_codab_to_blob():
    url = "https://data.fieldmaps.io/cod/originals/nga.shp.zip"
    response = requests.get(url)
    response.raise_for_status()
    blob_name = f"{src.constants.PROJECT_PREFIX}/raw/codab/nga.shp.zip"
    blob.upload_blob_data(blob_name, response.content)


def load_codab_from_blob(admin_level: int = 0, aoi_only: bool = False):
    shapefile = f"nga_adm{admin_level}.shp"
    gdf = stratus.load_shp_from_blob(
        f"{src.constants.PROJECT_PREFIX}/raw/codab/nga.shp.zip",
        shapefile=shapefile,
        stage="dev",
    )
    if aoi_only:
        gdf = gdf[gdf["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
    return gdf
