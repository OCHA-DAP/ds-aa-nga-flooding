import os
from pathlib import Path

import geopandas as gpd

from src.constants import ADAMAWA
from src.datasources import codab

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
HS_RAW_DIR = DATA_DIR / "public" / "raw" / "glb" / "hydrosheds"
RIVERS_DIR = HS_RAW_DIR / "HydroRIVERS_v10_af_shp" / "HydroRIVERS_v10_af_shp"
HS_PROC_DIR = DATA_DIR / "public" / "processed" / "glb" / "hydrosheds"
HS_NGA_PROC_DIR = DATA_DIR / "public" / "processed" / "nga" / "hydrosheds"


def load_all_rivers():
    return gpd.read_file(RIVERS_DIR)


def load_niger_river():
    filename = "niger_river"
    return gpd.read_file(HS_PROC_DIR / filename)


def load_niger_system_rivers():
    filename = "niger_system_rivers"
    return gpd.read_file(HS_PROC_DIR / filename)


def process_benue_aoi():
    adm2 = codab.load_codab(admin_level=2)
    adm2_a = adm2[adm2["ADM1_PCODE"] == ADAMAWA]
    niger_system = load_niger_system_rivers()
    ada_rivers = gpd.sjoin(
        niger_system, adm2_a, how="inner", predicate="intersects"
    )
    benue_aoi = ada_rivers[ada_rivers["DIS_AV_CMS"] > 600]

    filename = "benue_aoi"
    save_dir = HS_NGA_PROC_DIR / filename
    if not save_dir.exists():
        save_dir.mkdir(parents=True)
    benue_aoi.to_file(
        HS_NGA_PROC_DIR / filename / f"{filename}.shp",
        driver="ESRI Shapefile",
    )


def load_benue_aoi():
    filename = "benue_aoi"
    return gpd.read_file(HS_NGA_PROC_DIR / filename / f"{filename}.shp")
