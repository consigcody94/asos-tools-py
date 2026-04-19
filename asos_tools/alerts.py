"""Apprise-based alert dispatcher for O.W.L.

Supports 80+ services (Slack, Discord, Teams, email, SMS, webhooks, etc.)
via a single Apprise URL. Example Slack URL::

    slack://TokenA/TokenB/TokenC/Channel

Configure one or more URLs (comma-separated) via the ``OWL_ALERT_URLS``
environment variable, or pass them explicitly to :func:`send_alert`.

Typical O.W.L. call sites:

- Watchlist scan detects a new $ flag -> ``send_flag_alert(row)``
- Station goes silent for >2h -> ``send_missing_alert(row)``
- Manual test from Admin tab -> ``send_test_alert()``

All calls are fire-and-forget: failures are logged but never raise.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable, Mapping, Optional

import apprise

logger = logging.getLogger(__name__)

__all__ = [
    "send_alert",
    "send_flag_alert",
    "send_missing_alert",
    "send_test_alert",
    "load_urls_from_env",
    "SUPPORTED_SERVICES",
]


#: A few common services with example URL templates. Full list: apprise docs.
SUPPORTED_SERVICES: dict[str, str] = {
    "Slack":      "slack://TokenA/TokenB/TokenC/#channel",
    "Discord":    "discord://webhook_id/webhook_token",
    "Teams":      "msteams://TokenA/TokenB/TokenC/",
    "Email":      "mailto://user:pass@smtp.example.com?to=ops@domain",
    "Webhook":    "json://hostname/path",
    "PagerDuty":  "pagerduty://integration_key@api_key",
    "Telegram":   "tgram://bottoken/ChatID",
    "Pushover":   "pover://user_key@token",
}


def load_urls_from_env() -> list[str]:
    """Return Apprise URLs from ``OWL_ALERT_URLS`` (comma-separated)."""
    raw = os.environ.get("OWL_ALERT_URLS", "").strip()
    if not raw:
        return []
    return [u.strip() for u in raw.split(",") if u.strip()]


def _dispatch(
    title: str,
    body: str,
    urls: Iterable[str],
    *,
    notify_type: str = "info",
) -> tuple[int, int]:
    """Send ``(title, body)`` to each Apprise URL. Returns (sent, failed)."""
    apr = apprise.Apprise()
    added = 0
    for u in urls:
        try:
            if apr.add(u):
                added += 1
        except Exception:
            logger.exception("Failed to add Apprise URL")
    if not added:
        return 0, 0
    try:
        ok = apr.notify(
            title=title,
            body=body,
            notify_type=getattr(apprise.NotifyType, notify_type.upper(),
                                apprise.NotifyType.INFO),
        )
    except Exception:
        logger.exception("Apprise notify raised")
        return 0, added
    return (added if ok else 0), (0 if ok else added)


def send_alert(
    title: str,
    body: str,
    *,
    urls: Optional[Iterable[str]] = None,
    severity: str = "info",
) -> tuple[int, int]:
    """Low-level alert send. ``severity`` in {info, success, warning, failure}.

    Returns ``(sent, failed)`` counts; never raises.
    """
    urls = list(urls) if urls is not None else load_urls_from_env()
    if not urls:
        logger.info("send_alert called with no URLs; skipping")
        return 0, 0
    return _dispatch(title, body, urls, notify_type=severity)


def send_flag_alert(
    row: Mapping,
    *,
    urls: Optional[Iterable[str]] = None,
) -> tuple[int, int]:
    """Alert for a newly-flagged station. ``row`` is a watchlist row."""
    sid = row.get("station", "?")
    name = (row.get("name") or "").title()
    state = row.get("state") or ""
    reason = row.get("probable_reason") or "Internal check"
    metar = (row.get("latest_metar") or "")[:180]
    title = f"[O.W.L.] $ FLAG · {sid}"
    body = (
        f"Station: {sid} · {name}, {state}\n"
        f"Reason: {reason}\n"
        f"Latest METAR:\n{metar}\n"
        f"-- O.W.L. Observation Watch Log"
    )
    return send_alert(title, body, urls=urls, severity="warning")


def send_missing_alert(
    row: Mapping,
    *,
    urls: Optional[Iterable[str]] = None,
) -> tuple[int, int]:
    """Alert for a silent/missing station."""
    sid = row.get("station", "?")
    name = (row.get("name") or "").title()
    state = row.get("state") or ""
    gaps = row.get("missing", 0)
    since = row.get("minutes_since_last_report")
    since_txt = f"{int(since)} min" if since is not None else "unknown"
    title = f"[O.W.L.] MISSING · {sid}"
    body = (
        f"Station: {sid} · {name}, {state}\n"
        f"Missed hours: {gaps}\n"
        f"Minutes since last report: {since_txt}\n"
        f"-- O.W.L. Observation Watch Log"
    )
    return send_alert(title, body, urls=urls, severity="failure")


def send_test_alert(
    *,
    urls: Optional[Iterable[str]] = None,
) -> tuple[int, int]:
    """Send a 'hello, you're wired up' ping."""
    return send_alert(
        title="[O.W.L.] Test alert",
        body="This is a test notification from O.W.L. Observation Watch Log. "
             "If you see this, alerting is configured correctly.",
        urls=urls,
        severity="success",
    )
