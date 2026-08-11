"""Daily NHF flash-flood observational monitoring.

Reads per-LGA FloodScan exposure from the live floodexposure-monitoring
pipeline (DB app.floodscan_exposure, prod), evaluates the observational
trigger, saves the chart to blob, and sends Listmonk campaigns to the
flash-flood lists (separate audience from the riverine stream).

Emails go out when the trigger is reached or on Mondays. TEST behaviour
(STAGE != prod, the default): sends route to the flash test list, with a
"[TEST]" subject prefix and the campaign template's test banner.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import ocha_stratus as stratus
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from ocha_relay.listmonk import ListmonkClient

from src.constants import (
    FLASH_ROLLING_DAYS,
    LISTMONK_FLASH_LISTS,
    LISTMONK_PROJECT_TAG,
)
from src.monitoring import flash

load_dotenv()

TEMPLATES_DIR = Path("src/monitoring/email/templates/")


def resolve_list_id(client, list_type):
    """Resolve a flash-flood listmonk list id by its type tag."""
    tag = LISTMONK_FLASH_LISTS[list_type]["tag"]
    for lst in client.fetch_all_lists(tag=LISTMONK_PROJECT_TAG):
        if tag in lst.get("tags", []):
            return lst["id"]
    raise RuntimeError(
        f"No listmonk list tagged {tag!r}. Run "
        "pipelines/setup_nga_listmonk_lists.py --flash first."
    )


if __name__ == "__main__":
    monitoring_date = os.getenv("MONITORING_DATE", "")
    if not monitoring_date:
        monitoring_date = datetime.today().strftime("%Y-%m-%d")
    monitoring_date_obj = datetime.strptime(monitoring_date, "%Y-%m-%d")

    df = flash.load_exposure()
    status = flash.evaluate_flash(df)
    latest_date = status["latest_date"].strftime("%Y-%m-%d")
    print(
        f"Flash status for {monitoring_date} "
        f"(exposure to {latest_date}): "
        f"triggered={status['triggered']}, lgas="
        + ", ".join(
            f"{v['name']}={v['rolling']:,.0f}"
            for v in status["lgas"].values()
            if v["rolling"] is not None
        )
    )

    flash.flash_plot(df, status, save_output=True)

    if status["triggered"]:
        trigger_status = "FLASH FLOOD TRIGGER REACHED"
        template_name = "flash_action"
        email_type = "trigger"
    else:
        trigger_status = "NOT ACTIVATED"
        template_name = "flash_informational"
        email_type = "info"

    # Send emails if the trigger has been reached, or if it is a Monday
    if status["triggered"] or monitoring_date_obj.weekday() == 0:
        print(f"Sending emails for date: {monitoring_date}")
        stage = os.getenv("STAGE", "dev")
        test = False if stage == "prod" else True
        if test:
            print("This is a TEST email!")

        client = ListmonkClient.from_env()

        blob_name = flash.get_flash_plot_blob_name(
            latest_date, status["triggered"]
        )
        chart_bytes = (
            stratus.get_container_client()
            .get_blob_client(blob_name)
            .download_blob()
            .readall()
        )
        chart_url = client.upload_media(
            chart_bytes, f"nga_flash_monitoring_{monitoring_date}.png"
        )

        environment = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
        template = environment.get_template(f"{template_name}.html")
        body = template.render(
            pub_date=monitoring_date,
            latest_date=latest_date,
            trigger_status=trigger_status,
            chart_url=chart_url,
            lgas=status["lgas"],
            thresholds_pending=status["thresholds_pending"],
            rolling_days=FLASH_ROLLING_DAYS,
            cadence_note=(
                "Email updates are sent weekly on Mondays, or in the event "
                "that the flash flooding trigger is met."
            ),
        )

        campaign_name = (
            f"{LISTMONK_PROJECT_TAG} {template_name} {monitoring_date} "
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}"
        )
        if test:
            campaign_name += " [test]"

        test_text = "[TEST] " if test else ""
        subject = (
            f"{test_text}Nigeria AA: Flash Flooding"
            f" - {trigger_status} {monitoring_date}"
        )
        list_id = resolve_list_id(client, "test" if test else email_type)
        campaign_id = client.create_campaign(
            name=campaign_name,
            subject=subject,
            body=body,
            list_ids=[list_id],
        )  # default template_id = the instance's base_campaign
        client.send_campaign(campaign_id, skip_confirmation=True)
        print(
            f"Sent {template_name} campaign {campaign_id} to list {list_id}"
        )
    else:
        print(f"Not sending email. Trigger status is {trigger_status}")
