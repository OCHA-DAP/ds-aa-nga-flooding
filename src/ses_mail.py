"""Direct SMTP send through the team's AWS SES account (the humdata sender).

Stop-gap backend for while Listmonk is unavailable (it lives on the dev DB,
which lost public network access on 2026-09-22). Select it with
``EMAIL_BACKEND=ses``; the default backend stays Listmonk.

Differences from the Listmonk path, by design:
- No subscriber lists: recipients are an explicit address list
  (``SES_RECIPIENTS`` env, comma-separated, else the constants defaults;
  TEST_EMAIL / --send-test narrow it to the test recipients).
- Chart images travel as CID inline attachments (``multipart/related``)
  rather than Listmonk media URLs. Base64 ``data:`` URIs are NOT used
  because Gmail and Outlook refuse to render them.
- No Listmonk campaign template chrome (header/footer/unsubscribe): the
  bare pipeline body is wrapped in a minimal ``<html>`` shell.

Credentials come from the ``DSCI_AWS_EMAIL_*`` env vars (locally: the
team dotfiles; on Databricks: the ``dsci`` secret scope via the job
wrapper).
"""

from __future__ import annotations

import base64
import logging
import os
import re
import smtplib
from collections.abc import Iterable
from email.message import EmailMessage
from email.utils import make_msgid

logger = logging.getLogger(__name__)

_B64_IMG_RE = re.compile(r"data:image/png;base64,([A-Za-z0-9+/=]+)")
_XLSX_MIME = ("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def recipients_from_env(
    default: Iterable[str], var: str = "SES_RECIPIENTS"
) -> list[str]:
    """Comma-separated ``var`` if set and non-empty, else ``default``."""
    raw = os.environ.get(var, "").strip()
    if raw:
        return [a.strip() for a in raw.split(",") if a.strip()]
    return list(default)


def wrap_html(body: str, max_width_px: int = 900) -> str:
    """Minimal document shell around a pipeline body fragment (mirrors the
    ``--preview --raw`` wrapper)."""
    style = f"font-family:sans-serif;max-width:{max_width_px}px;margin:auto"
    return (
        "<html><head><meta charset='utf-8'></head>"
        f"<body style='{style}'>{body}</body></html>"
    )


def send_via_ses(
    subject: str,
    html: str,
    recipients: list[str],
    attachments: Iterable[tuple[str, bytes]] = (),
    text_fallback: str | None = None,
) -> str:
    """Send one HTML email; returns the Message-ID.

    ``html`` may carry ``data:image/png;base64,...`` sources — each distinct
    image becomes one CID-referenced inline part.
    """
    if not recipients:
        raise ValueError("send_via_ses: no recipients")
    host = os.environ["DSCI_AWS_EMAIL_HOST"]
    sender = os.environ["DSCI_AWS_EMAIL_ADDRESS"]
    user = os.environ["DSCI_AWS_EMAIL_USERNAME"]
    password = os.environ["DSCI_AWS_EMAIL_PASSWORD"]
    port = int(os.environ.get("DSCI_AWS_EMAIL_PORT", "587"))

    cids: dict[str, tuple[str, bytes]] = {}

    def _to_cid(m: re.Match) -> str:
        b64 = m.group(1)
        if b64 not in cids:
            cid = make_msgid(domain="ses.humdata.org")
            cids[b64] = (cid, base64.b64decode(b64))
        return f"cid:{cids[b64][0][1:-1]}"

    html = _B64_IMG_RE.sub(_to_cid, html)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Message-ID"] = make_msgid(domain="ses.humdata.org")
    msg.set_content(text_fallback or "This email is best viewed as HTML.")
    msg.add_alternative(html, subtype="html")
    html_part = msg.get_payload()[-1]
    for cid, png in cids.values():
        html_part.add_related(png, maintype="image", subtype="png", cid=cid)
    for filename, data in attachments:
        msg.add_attachment(
            data, maintype=_XLSX_MIME[0], subtype=_XLSX_MIME[1], filename=filename
        )

    with smtplib.SMTP(host, port, timeout=120) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        refused = smtp.send_message(msg)
    if refused:
        logger.warning(f"SES refused some recipients: {refused}")
    logger.info(
        f"Sent via SES to {recipients} ({len(cids)} inline images, "
        f"{len(list(attachments))} attachments): {subject!r}"
    )
    return msg["Message-ID"]
