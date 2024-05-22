import os
from pathlib import Path

import geopandas as gpd

from src.constants import AOI_ADM1_PCODES

DATA_DIR = Path(os.getenv("AA_DATA_DIR"))
CODAB_DIR = DATA_DIR / "public" / "raw" / "nga" / "cod_ab"


def load_codab(admin_level: int = 0, aoi_only: bool = False):
    filename = f"nga_admbnda_adm{admin_level}_osgof_20190417.shp"
    gdf = gpd.read_file(CODAB_DIR / filename)
    if aoi_only:
        gdf = gdf[gdf["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
    return gdf
