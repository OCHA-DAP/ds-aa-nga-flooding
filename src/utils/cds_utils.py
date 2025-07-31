import os
from pathlib import Path
from typing import Literal

import cdsapi
import ocha_stratus as stratus


def download_raw_cds_api_to_blob(
    dataset: str,
    request: dict,
    blob_name: str,
    prod_dev: Literal["prod", "dev"] = "dev",
    keep_local_copy: bool = True,
):
    local_filepath = "temp" / Path(blob_name)
    if not local_filepath.parent.exists():
        os.makedirs(local_filepath.parent)
    c = cdsapi.Client()
    response = c.retrieve(dataset, request)
    response.download(local_filepath)
    with open(local_filepath, "rb") as file:
        stratus.upload_blob_data(file, blob_name, stage=prod_dev)
    if not keep_local_copy:
        os.remove(local_filepath)
    return local_filepath if keep_local_copy else None
