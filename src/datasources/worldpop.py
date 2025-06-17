import os
from pathlib import Path

import pandas as pd
import rioxarray as rxr

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
