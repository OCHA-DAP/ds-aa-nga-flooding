import os
from pathlib import Path

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
RAW_IMPACT_DIR = DATA_DIR / "public" / "raw" / "nga" / "ocha"
NEMA_RAW_IMPACT_PATH = (
    DATA_DIR
    / "public"
    / "raw"
    / "nga"
    / "nema"
    / "nema-flood-data-06102022.xlsx"
)
