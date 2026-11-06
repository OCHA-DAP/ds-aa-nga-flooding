import os
import time
from pathlib import Path
from typing import Literal

import cdsapi
import ocha_stratus as stratus

try:  # the cdsapi >= 0.7.5 backend
    from ecmwf.datastores.processing import ProcessingFailedError
except ImportError:  # older backends raise a RuntimeError subclass too
    ProcessingFailedError = RuntimeError

# For the bounded path: cap the client's own per-call retries. cdsapi's
# defaults (500 tries, 120 s apart) let one status call block for ~16 h,
# past any deadline. Transient poll errors are retried by _wait_or_delete
# until its deadline instead.
_BOUNDED_CLIENT_KWARGS = {"retry_max": 20, "sleep_max": 60}


def _try_delete(remote) -> None:
    try:
        remote.delete()
        print(f"Deleted request {remote.request_id}")
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: could not delete {remote.request_id}: {exc}")


def _wait_or_delete(remote, max_wait_minutes: float) -> None:
    """Poll a submitted request until it is ready, or raise
    ``TimeoutError`` once ``max_wait_minutes`` have passed.

    Whenever this exits without the results being ready (deadline, a
    failed/rejected request, an exception, an interrupt), the request is
    deleted so it doesn't stay queued on the account. EWDS runs one job at
    a time per account, so an orphan would hold up the next run and the
    Somalia monitoring on the same service. A status call that errors
    (EWDS 5xx, connection drop) is retried until the deadline. A
    failed/rejected request raises at once.
    """
    deadline = time.monotonic() + max_wait_minutes * 60
    sleep = 5.0
    try:
        while True:
            try:
                if remote.results_ready:
                    return
            except ProcessingFailedError:
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: status check failed, will retry: {exc}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"request {remote.request_id} not ready after "
                    f"{max_wait_minutes:.0f} min"
                )
            time.sleep(min(sleep, remaining))
            sleep = min(sleep * 1.5, 120)
    except BaseException:
        _try_delete(remote)
        raise


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
        c = cdsapi.Client(wait_until_complete=False, **_BOUNDED_CLIENT_KWARGS)
        response = c.retrieve(dataset, request)
        _wait_or_delete(response, max_wait_minutes)
    response.download(local_filepath)
    with open(local_filepath, "rb") as file:
        stratus.upload_blob_data(file, blob_name, stage=prod_dev)
    if not keep_local_copy:
        os.remove(local_filepath)
    return local_filepath if keep_local_copy else None
