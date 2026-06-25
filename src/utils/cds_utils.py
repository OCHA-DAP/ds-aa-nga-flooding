import os
import tempfile
from pathlib import Path
from typing import Literal

import cdsapi
import ocha_stratus as stratus


def _make_cds_client(url: str = None) -> cdsapi.Client:
    """Instantiate a CDS API client.

    On Databricks, CDSAPI_URL and CDSAPI_KEY are injected as env vars by the
    cluster policy. Locally, falls back to ~/.cdsapirc. Pass url to override
    the env var (e.g. when the policy enforces an endpoint that doesn't serve
    a particular dataset).
    """
    resolved_url = url or os.getenv("CDSAPI_URL")
    key = os.getenv("CDSAPI_KEY")
    if resolved_url and key:
        return cdsapi.Client(url=resolved_url, key=key)
    return cdsapi.Client()


def download_raw_cds_api_to_blob(
    dataset: str,
    request: dict,
    blob_name: str,
    prod_dev: Literal["prod", "dev"] = "dev",
    keep_local_copy: bool = True,
    cds_url: str = None,
):
    # Use /tmp on Databricks (repo checkout dir is read-only);
    # respect CDS_TEMP_DIR env var or fall back to "temp" locally.
    base = Path(os.getenv("CDS_TEMP_DIR", tempfile.gettempdir()))
    local_filepath = base / Path(blob_name)
    local_filepath.parent.mkdir(parents=True, exist_ok=True)
    c = _make_cds_client(url=cds_url)
    response = c.retrieve(dataset, request)
    response.download(local_filepath)
    with open(local_filepath, "rb") as file:
        stratus.upload_blob_data(file, blob_name, stage=prod_dev)
    if not keep_local_copy:
        os.remove(local_filepath)
    return local_filepath if keep_local_copy else None
