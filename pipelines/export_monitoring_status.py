"""Export a small status.json + latest chart snapshot for the static 2026
monitoring status page (exploration/2026/cerf/monitoring/).

Reuses the same evaluation functions the email pipelines call
(src.monitoring.etl.evaluate_trigger, src.monitoring.flash.evaluate_flash)
so the numbers on the static page always match what went out in the last
monitoring email — this script only adds a JSON/image export step, it does
not recompute trigger logic independently.

Run once per pipeline, each invocation only touches its own section of
status.json (the two pipelines run on independent schedules):

    python pipelines/export_monitoring_status.py riverine
    python pipelines/export_monitoring_status.py flash
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import ocha_stratus as stratus
from dotenv import load_dotenv

from src.constants import PROJECT_PREFIX
from src.monitoring import etl, flash

load_dotenv()

OUT_DIR = Path("exploration/2026/cerf/monitoring")
STATUS_PATH = OUT_DIR / "status.json"


def _download_blob(blob_name, dest_path):
    container = stratus.get_container_client("projects", "dev")
    data = container.get_blob_client(blob_name).download_blob().readall()
    dest_path.write_bytes(data)


def _load_status():
    if STATUS_PATH.exists():
        return json.loads(STATUS_PATH.read_text())
    return {}


def export_riverine():
    monitoring_date = etl.get_latest_monitoring_date()
    df = etl.get_database_forecast(monitoring_date)
    result = etl.evaluate_trigger(df)
    date_str = monitoring_date.strftime("%Y-%m-%d")

    blob_name = (
        f"{PROJECT_PREFIX}/monitoring/{date_str}_{result['action']}.png"
    )
    _download_blob(blob_name, OUT_DIR / "riverine_latest.png")

    return {
        "date": date_str,
        "action": result["action"],
        "readiness": result["readiness"],
        "readiness_forecast": result["readiness_forecast"],
        "readiness_reanalysis": result["readiness_reanalysis"],
        "max_gauges_exceeding": result["max_gauges_exceeding"],
        "n_gauges_reporting": result["n_gauges_reporting"],
        "glofas_max": result["glofas_max"],
        "reanalysis_max": result["reanalysis_max"],
        "chart": "riverine_latest.png",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def export_flash():
    df = flash.load_exposure()
    status = flash.evaluate_flash(df)
    date_str = status["latest_date"].strftime("%Y-%m-%d")

    blob_name = flash.get_flash_plot_blob_name(date_str, status["triggered"])
    _download_blob(blob_name, OUT_DIR / "flash_latest.png")

    return {
        "date": date_str,
        "triggered": status["triggered"],
        "warning": status["warning"],
        "thresholds_pending": status["thresholds_pending"],
        "lgas": {
            pcode: {
                "name": lga["name"],
                "rolling": lga["rolling"],
                "threshold": lga["threshold"],
                "exceeds": lga["exceeds"],
                "warning": lga["warning"],
            }
            for pcode, lga in status["lgas"].items()
        },
        "chart": "flash_latest.png",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("section", choices=["riverine", "flash"])
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    combined = _load_status()

    if args.section == "riverine":
        combined["riverine"] = export_riverine()
    else:
        combined["flash"] = export_flash()

    STATUS_PATH.write_text(json.dumps(combined, indent=2) + "\n")
    print(f"Wrote {STATUS_PATH} (section: {args.section})")
