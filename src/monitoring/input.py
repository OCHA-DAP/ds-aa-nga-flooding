import argparse
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check flooding forecast for a specific date"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Date in YYYY-MM-DD format (default: today)",
    )
    return parser.parse_args()
