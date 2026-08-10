"""Send monitoring emails as Listmonk campaigns (ocha_relay).

Campaigns are rendered against the Listmonk instance's default
``base_campaign`` template (which supplies the document, OCHA branding,
footer and unsubscribe link); the Jinja templates here provide only the
content fragment. The chart is uploaded to the Listmonk media library and
referenced by hosted URL.

TEST behaviour (STAGE != prod, the default): the campaign is sent to the
project's test list instead of the real info/trigger audience, the subject
gets a "[TEST]" prefix, and "[test]" in the campaign name switches on the
base_campaign template's test banner.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import ocha_stratus as stratus
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from ocha_relay.listmonk import ListmonkClient

from src.constants import (
    ACTION_MIN_GAUGES,
    LISTMONK_LISTS,
    LISTMONK_PROJECT_TAG,
)
from src.monitoring import etl, utils

load_dotenv()

TEMPLATES_DIR = Path("src/monitoring/email/templates/")


def resolve_list_id(client, list_type):
    """Resolve a listmonk list id by its type tag."""
    tag = LISTMONK_LISTS[list_type]["tag"]
    for lst in client.fetch_all_lists(tag=LISTMONK_PROJECT_TAG):
        if tag in lst.get("tags", []):
            return lst["id"]
    raise RuntimeError(
        f"No listmonk list tagged {tag!r}. Run "
        "pipelines/setup_nga_listmonk_lists.py first."
    )


if __name__ == "__main__":
    monitoring_date = os.getenv("MONITORING_DATE", "")
    if not monitoring_date:
        monitoring_date = datetime.today().strftime("%Y-%m-%d")

    monitoring_date_obj = datetime.strptime(monitoring_date, "%Y-%m-%d")

    status = etl.check_trigger_status(monitoring_date)
    action = status["action"]
    readiness = status["readiness"]

    if action:
        trigger_status = "ACTION TRIGGER REACHED"
        template_name = "action"
        email_type = "trigger"
    elif readiness:
        trigger_status = "READINESS TRIGGER REACHED"
        template_name = "readiness"
        email_type = "info"
    else:
        trigger_status = "NOT ACTIVATED"
        template_name = "informational"
        email_type = "info"

    # Send emails if a trigger has been reached, or if it is a Monday
    if action or readiness or monitoring_date_obj.weekday() == 0:
        print(f"Sending emails for date: {monitoring_date}")
        stage = os.getenv("STAGE", "dev")
        test = False if stage == "prod" else True
        if test:
            print("This is a TEST email!")

        client = ListmonkClient.from_env()

        # Chart is included on non-action emails only; hosted on the
        # listmonk media library rather than embedded
        chart_url = None
        if not action:
            blob_name = utils.get_plot_blob_name(monitoring_date, action)
            chart_bytes = (
                stratus.get_container_client()
                .get_blob_client(blob_name)
                .download_blob()
                .readall()
            )
            chart_url = client.upload_media(
                chart_bytes, f"nga_flooding_monitoring_{monitoring_date}.png"
            )

        environment = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
        template = environment.get_template(f"{template_name}.html")
        body = template.render(
            pub_date=monitoring_date,
            trigger_status=trigger_status,
            chart_url=chart_url,
            max_gauges_exceeding=status["max_gauges_exceeding"],
            n_gauges_reporting=status["n_gauges_reporting"],
            min_gauges=ACTION_MIN_GAUGES,
            readiness_forecast=status["readiness_forecast"],
            readiness_reanalysis=status["readiness_reanalysis"],
        )

        # "[test]" in the campaign name switches on the base_campaign
        # template's test banner
        campaign_name = (
            f"{LISTMONK_PROJECT_TAG} {template_name} {monitoring_date} "
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}"
        )
        if test:
            campaign_name += " [test]"

        list_id = resolve_list_id(client, "test" if test else email_type)
        campaign_id = client.create_campaign(
            name=campaign_name,
            subject=utils.get_email_subject(
                trigger_status, test, monitoring_date
            ),
            body=body,
            list_ids=[list_id],
        )  # default template_id = the instance's base_campaign
        client.send_campaign(campaign_id, skip_confirmation=True)
        print(
            f"Sent {template_name} campaign {campaign_id} to list {list_id}"
        )
    else:
        print(f"Not sending email. Trigger status is {trigger_status}")
