import re

from src.constants import PROJECT_PREFIX


def is_valid_email(email):
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    if re.match(email_regex, email):
        return True
    else:
        return False


def get_plot_blob_name(issue_time, trigger_status):
    return f"{PROJECT_PREFIX}/monitoring/{issue_time}_{trigger_status}.png"


def get_email_subject(trigger_status, test, monitoring_date):
    test_text = "[TEST] " if test else ""
    return (
        f"{test_text}Nigeria AA: Adamawa Riverine Flooding"
        f" - {trigger_status} {monitoring_date}"
    )
