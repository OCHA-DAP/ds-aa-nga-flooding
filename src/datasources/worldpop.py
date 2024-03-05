import os
from pathlib import Path

import rioxarray as rxr

AA_DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
RAW_WP_PATH = (
    AA_DATA_DIR
    / "public"
    / "raw"
    / "nga"
    / "worldpop"
    / "nga_ppp_2020_1km_Aggregated_UNadj.tif"
)


def load_raw_worldpop():
    da = rxr.open_rasterio(RAW_WP_PATH)
    return da.where(da != da.attrs["_FillValue"])
