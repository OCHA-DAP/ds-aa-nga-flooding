import os
import re
import smtplib
import ssl

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

from src.utils.blob import PROJECT_PREFIX

load_dotenv()

TEST_LIST = os.getenv("TEST_LIST", True)
EMAIL_HOST = os.getenv("DSCI_AWS_EMAIL_HOST")
EMAIL_PORT = int(os.getenv("DSCI_AWS_EMAIL_PORT"))
EMAIL_PASSWORD = os.getenv("DSCI_AWS_EMAIL_PASSWORD")
EMAIL_USERNAME = os.getenv("DSCI_AWS_EMAIL_USERNAME")
EMAIL_ADDRESS = os.getenv("DSCI_AWS_EMAIL_ADDRESS")


def process_distribution_list(test_list=TEST_LIST):
    distribution_list = get_distribution_list(test_list)
    valid_distribution_list = distribution_list[
        distribution_list["email"].apply(is_valid_email)
    ]
    invalid_distribution_list = distribution_list[
        ~distribution_list["email"].apply(is_valid_email)
    ]
    if not invalid_distribution_list.empty:
        print(
            f"Invalid emails found in distribution list: "
            f"{invalid_distribution_list['email'].tolist()}"
        )
    to_list = valid_distribution_list[valid_distribution_list["info"] == "to"]
    cc_list = valid_distribution_list[valid_distribution_list["info"] == "cc"]
    return {"to": to_list, "cc": cc_list}


def get_distribution_list(test_list) -> pd.DataFrame:
    """Load distribution list from blob storage."""
    if test_list:
        print("Using test distribution list")
        blob_name = f"{PROJECT_PREFIX}/email/test_distribution_list.csv"
    else:
        blob_name = f"{PROJECT_PREFIX}/email/distribution_list.csv"
    return stratus.load_csv_from_blob(blob_name)


def is_valid_email(email):
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    if re.match(email_regex, email):
        return True
    else:
        return False


def get_plot_blob_name(issue_time, trigger_status):
    return f"{PROJECT_PREFIX}/monitoring/{issue_time}_{trigger_status}.png"


def send_email(msg, to_list, cc_list):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=context) as server:
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(
            EMAIL_ADDRESS,
            to_list["email"].tolist() + cc_list["email"].tolist(),
            msg.as_string(),
        )
    print("Email sent!")


def get_email_subject(trigger_status, test, monitoring_date):
    test_text = "TEST" if test else ""
    trigger_text = "ACTIVATED" if trigger_status else "NOT ACTIVATED"
    return (
        f"{test_text}: Nigeria Anticipatory Action"
        f" - {trigger_text} {monitoring_date}"
    )
