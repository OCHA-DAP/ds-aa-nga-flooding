import io
import os
from datetime import datetime
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path

import ocha_stratus as stratus
from dotenv import load_dotenv
from html2text import html2text
from jinja2 import Environment, FileSystemLoader

from src.monitoring import etl, utils

load_dotenv()

STATIC_DIR = Path("src/monitoring/email/static/")
TEMPLATES_DIR = Path("src/monitoring/email/templates/")

if __name__ == "__main__":
    monitoring_date = os.getenv(
        "MONITORING_DATE", datetime.today().strftime("%Y-%m-%d")
    )
    date_obj = datetime.strptime(monitoring_date, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%b %d, %Y")

    stage = os.getenv("STAGE", "dev")
    test = False if stage == "prod" else True
    print(f"Sending emails for date: {monitoring_date}")
    if test:
        print("This is a TEST email!")

    results = etl.check_results(monitoring_date)
    overall_exceeds = results["google"] | results["glofas"]
    trigger_status = "ACTIVATED" if overall_exceeds else "NOT ACTIVATED"

    chd_banner_cid = make_msgid(domain="humdata.org")
    ocha_logo_cid = make_msgid(domain="humdata.org")
    chart_cid = make_msgid(domain="humdata.org")

    template_name = "informational" if not overall_exceeds else "action"
    environment = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    template = environment.get_template(f"{template_name}.html")

    email_type = "info" if not overall_exceeds else "trigger"
    distribution = utils.process_distribution_list(test, email_type)

    msg = EmailMessage()
    msg.set_charset("utf-8")
    msg["Subject"] = utils.get_email_subject(
        overall_exceeds, test, formatted_date
    )
    msg["From"] = Address(
        "OCHA Centre for Humanitarian Data",
        utils.EMAIL_ADDRESS.split("@")[0],
        utils.EMAIL_ADDRESS.split("@")[1],
    )
    msg["To"] = [
        Address(
            row["name"], row["email"].split("@")[0], row["email"].split("@")[1]
        )
        for _, row in distribution["to"].iterrows()
    ]
    msg["Cc"] = [
        Address(
            row["name"], row["email"].split("@")[0], row["email"].split("@")[1]
        )
        for _, row in distribution["cc"].iterrows()
    ]

    html_str = template.render(
        pub_date=formatted_date,
        chd_banner_cid=chd_banner_cid[1:-1],
        ocha_logo_cid=ocha_logo_cid[1:-1],
        chart_cid=chart_cid[1:-1],  # Don't need if triggering
        test_email=test,
        trigger_status=trigger_status,
    )

    text_str = html2text(html_str)
    msg.set_content(text_str)
    msg.add_alternative(html_str, subtype="html")

    if not overall_exceeds:
        blob_name = utils.get_plot_blob_name(monitoring_date, overall_exceeds)
        image_data = io.BytesIO()
        blob_client = stratus.get_container_client().get_blob_client(blob_name)
        blob_client.download_blob().download_to_stream(image_data)
        image_data.seek(0)
        msg.get_payload()[1].add_related(
            image_data.read(), "image", "png", cid=chart_cid
        )

    for filename, cid in zip(
        ["centre_banner.png", "ocha_logo_wide.png"],
        [chd_banner_cid, ocha_logo_cid],
    ):
        img_path = STATIC_DIR / filename
        with open(img_path, "rb") as img:
            msg.get_payload()[1].add_related(
                img.read(), "image", "png", cid=cid
            )

    utils.send_email(msg, distribution["to"], distribution["cc"])
