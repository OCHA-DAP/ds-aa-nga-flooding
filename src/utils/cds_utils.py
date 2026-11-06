import os
import time
from pathlib import Path
from typing import Literal

import cdsapi
import ocha_stratus as stratus


def _wait_or_delete(remote, max_wait_minutes: float) -> None:
    """Poll a submitted request until it is ready, or delete it and raise
    ``TimeoutError`` once ``max_wait_minutes`` have passed.

    The request is deleted rather than abandoned so it doesn't stay queued
    on the account (EWDS runs one job at a time per account; an orphan
    would hold up the next run, and the Somalia monitoring on the same
    service). ``failed`` / ``rejected`` raise from ``results_ready`` as
    they would in a plain blocking retrieve.
    """
    deadline = time.monotonic() + max_wait_minutes * 60
    sleep = 5.0
    while not remote.results_ready:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            try:
                remote.delete()
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: could not delete {remote.request_id}: {exc}")
            raise TimeoutError(
                f"request {remote.request_id} not ready after "
                f"{max_wait_minutes:.0f} min; deleted it"
            )
        time.sleep(min(sleep, remaining))
        sleep = min(sleep * 1.5, 120)


def download_raw_cds_api_to_blob(
    dataset: str,
    request: dict,
    blob_name: str,
    prod_dev: Literal["prod", "dev"] = "dev",
    keep_local_copy: bool = True,
    max_wait_minutes: float | None = None,
):
    """Retrieve ``request`` from the CDS/EWDS API and upload it to blob.

    ``max_wait_minutes=None`` blocks until the request is served, however
    long the queue. Otherwise the request is deleted and ``TimeoutError``
    raised once that many minutes have passed.
    """
    local_filepath = "temp" / Path(blob_name)
    if not local_filepath.parent.exists():
        os.makedirs(local_filepath.parent)
    if max_wait_minutes is None:
        c = cdsapi.Client()
        response = c.retrieve(dataset, request)
    else:
        # Submit without waiting, then poll with a deadline
        c = cdsapi.Client(wait_until_complete=False)
        response = c.retrieve(dataset, request)
        _wait_or_delete(response, max_wait_minutes)
    response.download(local_filepath)
    with open(local_filepath, "rb") as file:
        stratus.upload_blob_data(file, blob_name, stage=prod_dev)
    if not keep_local_copy:
        os.remove(local_filepath)
    return local_filepath if keep_local_copy else None
