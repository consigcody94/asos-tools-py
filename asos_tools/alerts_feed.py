"""NWS CAP (Common Alerting Protocol) active-alerts feed.

Thin wrapper over ``https://api.weather.gov/alerts/active`` (free, no auth).
Returns weather watches, warnings, and advisories as structured dicts
suitable for joining with AOMC stations by state or coordinates.

Usage::

    from asos_tools.alerts_feed import (
        fetch_active_alerts, alerts_for_state, alerts_for_station,
    )
    alerts = fetch_active_alerts()
    ny_alerts = alerts_for_state("NY")
    jfk_alerts = alerts_for_station({"id": "KJFK", "state": "NY",
                                     "lat": 40.64, "lon": -73.78})
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from functools import lru_cache
import time
from typing import Iterable, Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "fetch_active_alerts",
    "alerts_for_state",
    "alerts_for_station",
    "SEVERITY_ORDER",
]


_UA = "O.W.L./1.0 (contact: rockvillecodymaryland@gmail.com)"
_BASE = os.environ.get("NWS_API_BASE", "https://api.weather.gov")

SEVERITY_ORDER = ("Extreme", "Severe", "Moderate", "Minor", "Unknown")


def _parse(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


@lru_cache(maxsize=1)
def _cached_alerts(ts_bucket: int) -> list[dict]:
    """Fetch active alerts. Cached per 2-minute bucket."""
    url = f"{_BASE.rstrip('/')}/alerts/active"
    params = {"status": "actual"}
    try:
        r = requests.get(url, params=params, timeout=15,
                         headers={"User-Agent": _UA,
                                  "Accept": "application/geo+json"})
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception("NWS CAP fetch failed")
        return []

    out = []
    for feat in (data.get("features") or []):
        p = feat.get("properties") or {}
        out.append({
            "id": p.get("id"),
            "event": p.get("event"),
            "severity": p.get("severity"),
            "urgency": p.get("urgency"),
            "certainty": p.get("certainty"),
            "area_desc": p.get("areaDesc"),
            "headline": p.get("headline"),
            "description": p.get("description"),
            "sent": _parse(p.get("sent")),
            "effective": _parse(p.get("effective")),
            "expires": _parse(p.get("expires")),
            "sender": p.get("senderName"),
            "affected_zones": p.get("affectedZones") or [],
            "same_codes": p.get("geocode", {}).get("SAME") or [],
            "ugc_codes": p.get("geocode", {}).get("UGC") or [],
            "link": p.get("@id") or "",
        })
    return out


def fetch_active_alerts() -> list[dict]:
    """Return all currently-active NWS CAP alerts (cached 2 min)."""
    return _cached_alerts(int(time.time()) // 120)


def alerts_for_state(state_code: str) -> list[dict]:
    """Filter alerts to those affecting the given 2-letter state code.

    The NWS feed uses UGC codes formatted ``{ST}Z{NNN}`` (zone) or
    ``{ST}C{NNN}`` (county). We match on the two-letter prefix.
    """
    st = (state_code or "").upper().strip()
    if not st:
        return []
    out = []
    for a in fetch_active_alerts():
        codes = list(a.get("ugc_codes") or [])
        if any(c.startswith(st) for c in codes):
            out.append(a)
    return out


def alerts_for_station(station: dict) -> list[dict]:
    """Filter alerts likely relevant to a single station (by state)."""
    state = (station or {}).get("state")
    if not state:
        return []
    return alerts_for_state(state)
