"""One-time setup: create the Nigeria flooding listmonk lists and (optionally)
migrate the existing distribution-list subscribers into them.

The legacy pipeline sent to two audiences from a blob CSV ("info" and
"trigger" columns). Listmonk has no to/cc concept, so each audience becomes
its own list (see src.constants.LISTMONK_LISTS), plus a test list that the
dispatch targets whenever STAGE != prod.

ocha_relay's ListmonkClient can create lists (create_list / fetch_all_lists)
but does NOT expose subscriber creation, so the subscriber import goes through
listmonk's HTTP API directly.

The script is idempotent: existing lists (matched by tag) are reused, and
existing subscribers (matched by email) are added to the target lists rather
than recreated. Run --dry-run first to preview.

Requires ADMIN listmonk credentials (creating lists/subscribers is a write,
which the send-scoped sender_api key cannot do): DSCI_LISTMONK_BASE_URL,
DSCI_LISTMONK_ADMIN_API_USERNAME, DSCI_LISTMONK_ADMIN_API_KEY. (The dispatch
in send_emails.py uses the send-scoped DSCI_LISTMONK_API_* via
ListmonkClient.from_env.)

Usage:
    python pipelines/setup_nga_listmonk_lists.py --dry-run     # preview only
    python pipelines/setup_nga_listmonk_lists.py --lists-only  # no subscribers
    python pipelines/setup_nga_listmonk_lists.py               # apply
"""

import argparse
import os
import sys
from pathlib import Path

import ocha_stratus as stratus
import requests
from dotenv import load_dotenv
from ocha_relay.listmonk import ListmonkClient

# Put the repo root on the path so `src` imports resolve when this script is
# run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.constants import (  # noqa: E402
    LISTMONK_FLASH_LISTS,
    LISTMONK_LISTS,
    LISTMONK_PROJECT_TAG,
    PROJECT_PREFIX,
)
from src.monitoring.utils import is_valid_email  # noqa: E402

load_dotenv()

DIST_LIST_BLOB = f"{PROJECT_PREFIX}/email/distribution_list.csv"
# distribution_list.csv columns map "to"/"cc" per email type; for listmonk
# both simply mean "receives this email", so we treat them identically.
_RECEIVES = ["to", "cc"]


def _admin_client() -> ListmonkClient:
    """ListmonkClient built from the ADMIN credentials."""
    return ListmonkClient(
        base_url=os.environ["DSCI_LISTMONK_BASE_URL"].rstrip("/"),
        username=os.environ["DSCI_LISTMONK_ADMIN_API_USERNAME"],
        password=os.environ["DSCI_LISTMONK_ADMIN_API_KEY"],
    )


def _listmonk_http():
    """(session, base_url) for direct listmonk subscriber calls, using the
    ADMIN credentials. base_url already includes the /api prefix."""
    base = os.environ["DSCI_LISTMONK_BASE_URL"].rstrip("/")
    session = requests.Session()
    session.auth = (
        os.environ["DSCI_LISTMONK_ADMIN_API_USERNAME"],
        os.environ["DSCI_LISTMONK_ADMIN_API_KEY"],
    )
    return session, base


def resolve_or_create_lists(
    client: ListmonkClient, dry_run: bool, lists_cfg=LISTMONK_LISTS
) -> dict:
    """Return {type: list_id} for the info/trigger/test lists, creating any
    that do not already exist (matched by their type tag)."""
    existing = client.fetch_all_lists(tag=LISTMONK_PROJECT_TAG)
    tag_to_id = {
        tag: lst["id"] for lst in existing for tag in lst.get("tags", [])
    }

    list_ids = {}
    for list_type, cfg in lists_cfg.items():
        if cfg["tag"] in tag_to_id:
            lid = tag_to_id[cfg["tag"]]
            list_ids[list_type] = lid
            print(f"  list '{cfg['name']}' exists (id={lid})")
        elif dry_run:
            list_ids[list_type] = None
            print(f"  + would create list '{cfg['name']}' tag={cfg['tag']}")
        else:
            tags = [LISTMONK_PROJECT_TAG, cfg["tag"]]
            tags += cfg.get("extra_tags", [])
            new_id = client.create_list(name=cfg["name"], tags=tags)
            list_ids[list_type] = new_id
            print(f"  + created list '{cfg['name']}' (id={new_id})")
    return list_ids


def load_target_memberships(dist_blob: str = DIST_LIST_BLOB) -> dict:
    """Read the distribution list and return {email: {"name", "types"}},
    where ``types`` is the set of audiences ("info"/"trigger") that email
    belongs to. Invalid emails are skipped."""
    df = stratus.load_csv_from_blob(dist_blob)
    df["email"] = df["email"].str.strip()

    memberships = {}
    skipped = set()
    for list_type in ("info", "trigger"):
        for _, row in df[df[list_type].isin(_RECEIVES)].iterrows():
            email = str(row["email"]).lower()
            if not is_valid_email(email):
                skipped.add(str(row["email"]))
                continue
            name = row.get("name")
            name = name if isinstance(name, str) and name.strip() else email
            entry = memberships.setdefault(
                email, {"name": name, "types": set()}
            )
            entry["types"].add(list_type)
    if skipped:
        print(f"  ! skipped {len(skipped)} invalid emails: {sorted(skipped)}")
    return memberships


def fetch_existing_subscribers(session, base) -> dict:
    """{email_lower: subscriber_id} for all current listmonk subscribers."""
    out, page = {}, 1
    while True:
        resp = session.get(
            f"{base}/subscribers",
            params={"page": page, "per_page": 1000},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        for row in data["results"]:
            out[row["email"].lower()] = row["id"]
        if page * data["per_page"] >= data["total"]:
            break
        page += 1
    return out


def import_subscribers(session, base, memberships, list_ids, dry_run):
    """Create new subscribers / add existing ones to the target lists."""
    existing = fetch_existing_subscribers(session, base)
    created = updated = 0
    for email, info in memberships.items():
        target_ids = [
            list_ids[t] for t in info["types"] if list_ids[t] is not None
        ]
        if email in existing:
            updated += 1
            if not dry_run and target_ids:
                session.put(
                    f"{base}/subscribers/lists",
                    json={
                        "ids": [existing[email]],
                        "action": "add",
                        "target_list_ids": target_ids,
                        "status": "confirmed",
                    },
                ).raise_for_status()
        else:
            created += 1
            if not dry_run and target_ids:
                session.post(
                    f"{base}/subscribers",
                    json={
                        "email": email,
                        "name": info["name"],
                        "status": "enabled",
                        "lists": target_ids,
                        # type=mailing_list suppresses the unsubscribe link
                        # in the campaign template. These are operational
                        # alert recipients we don't want self-unsubscribing.
                        "attribs": {"type": "mailing_list"},
                        "preconfirm_subscriptions": True,
                    },
                ).raise_for_status()
    verb = "would create" if dry_run else "created"
    verb2 = "would add to lists" if dry_run else "added to lists"
    print(f"  new subscribers ({verb}): {created}")
    print(f"  existing subscribers ({verb2}): {updated}")


def main(dry_run: bool = False, lists_only: bool = False, flash: bool = False):
    mode = "DRY RUN" if dry_run else "APPLY"
    stream = "flash flooding" if flash else "riverine flooding"
    print(f"=== Nigeria {stream} listmonk list setup ({mode}) ===")

    client = _admin_client()
    print("Resolving / creating lists:")
    lists_cfg = LISTMONK_FLASH_LISTS if flash else LISTMONK_LISTS
    list_ids = resolve_or_create_lists(client, dry_run, lists_cfg)

    if flash:
        # No legacy distribution CSV for the flash stream — manage
        # subscribers in the Listmonk UI (or via the admin API).
        print(f"List IDs: {list_ids}")
        print("Done.")
        return

    if lists_only:
        print("Lists only — skipping subscriber import.")
        print(f"List IDs: {list_ids}")
        print("Done.")
        return

    print("Mapping distribution-list subscribers:")
    memberships = load_target_memberships()
    n_info = sum("info" in m["types"] for m in memberships.values())
    n_trig = sum("trigger" in m["types"] for m in memberships.values())
    print(
        f"  {len(memberships)} unique subscribers "
        f"(info={n_info}, trigger={n_trig})"
    )

    print("Importing subscribers:")
    session, base = _listmonk_http()
    import_subscribers(session, base, memberships, list_ids, dry_run)
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview lists/subscribers without creating anything.",
    )
    parser.add_argument(
        "--lists-only",
        action="store_true",
        help="Create the lists but skip the subscriber import.",
    )
    parser.add_argument(
        "--flash",
        action="store_true",
        help="Create the flash-flood stream's lists instead (no subscriber "
        "import; audiences are managed directly in listmonk).",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run, lists_only=args.lists_only, flash=args.flash)
