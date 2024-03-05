import os
from pathlib import Path

import geopandas as gpd

DATA_DIR = Path(os.getenv("AA_DATA_DIR"))
CODAB_DIR = DATA_DIR / "public" / "raw" / "nga" / "cod_ab"


def load_codab(admin_level: int = 0):
    filename = f"nga_admbnda_adm{admin_level}_osgof_20190417.shp"
    gdf = gpd.read_file(CODAB_DIR / filename)
    return gdf
