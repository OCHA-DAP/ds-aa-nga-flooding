import os
from pathlib import Path
from typing import Literal

import cdsapi
import ocha_stratus as stratus


def _make_cds_client() -> cdsapi.Client:
    """Instantiate a CDS API client.

    On Databricks, CDSAPI_URL and CDSAPI_KEY are injected as env vars by the
    cluster policy. Locally, falls back to ~/.cdsapirc.
    """
    url = os.getenv("CDSAPI_URL")
    key = os.getenv("CDSAPI_KEY")
    if url and key:
        return cdsapi.Client(url=url, key=key)
    return cdsapi.Client()


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
    c = _make_cds_client()
    response = c.retrieve(dataset, request)
    response.download(local_filepath)
    with open(local_filepath, "rb") as file:
        stratus.upload_blob_data(file, blob_name, stage=prod_dev)
    if not keep_local_copy:
        os.remove(local_filepath)
    return local_filepath if keep_local_copy else None
