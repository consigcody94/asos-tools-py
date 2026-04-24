"""FAA NOTAM API client — planned-outage correlation.

The FAA NOTAM API (``external-api.faa.gov/notamapi/v1/notams``) exposes
active NOTAMs in ICAO format. It requires a free API key pair obtained
from the FAA NOTAM Search API developer portal.

Credentials are read from two env vars:

  - ``FAA_NOTAM_CLIENT_ID``
  - ``FAA_NOTAM_CLIENT_SECRET``

When unset, every function returns an empty list — the drill panel
gracefully renders a "configure FAA_NOTAM credentials to enable" notice
rather than the tile silently disappearing. This matches the policy in
``asos_tools/sources.py`` where the NOTAM feed is listed as an opt-in
source (free registration).

Why we want this in OWL:
    An ASOS site going MISSING during a **planned** airport equipment
    outage is NOT a sensor failure — it's a scheduled maintenance
    window. Correlating MISSING classifications against active NOTAMs
    for the ICAO's equipment codes (``NOTAM D`` / ``NAV`` / ``COM``)
    keeps AOMC from dispatching a truck roll for a no-op.
"""

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache
from typing import Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "is_configured",
    "fetch_notams_for_icao",
    "summarize_for_drill",
    "NOTAM_API_BASE",
]

NOTAM_API_BASE = "https://external-api.faa.gov/notamapi/v1/notams"

_HEADERS_BASE = {
    "User-Agent": "owl.observation-watch-log/1.0 (github.com/consigcody94/asos-tools-py)",
    "Accept": "application/json",
}


def is_configured() -> bool:
    """True iff both FAA_NOTAM credentials are present in the environment."""
    return bool(os.environ.get("FAA_NOTAM_CLIENT_ID") and
                os.environ.get("FAA_NOTAM_CLIENT_SECRET"))


def _auth_headers() -> dict:
    cid = os.environ.get("FAA_NOTAM_CLIENT_ID", "")
    sec = os.environ.get("FAA_NOTAM_CLIENT_SECRET", "")
    return {**_HEADERS_BASE, "client_id": cid, "client_secret": sec}


@lru_cache(maxsize=256)
def _cached_fetch(icao: str, bust: str) -> tuple:
    """``bust`` is the 10-min cache bucket. Returns tuple for hashability."""
    if not is_configured():
        return ()
    params = {"icaoLocation": icao, "responseFormat": "geoJson", "pageSize": 50}
    try:
        r = requests.get(NOTAM_API_BASE, headers=_auth_headers(),
                         params=params, timeout=15.0)
        if r.status_code != 200:
            logger.warning("FAA NOTAM API %s returned %s", icao, r.status_code)
            return ()
        data = r.json() or {}
    except Exception:
        logger.exception("FAA NOTAM fetch failed for %s", icao)
        return ()
    items = data.get("items") or []
    out: list[dict] = []
    for it in items:
        props = it.get("properties") or {}
        core = (props.get("coreNOTAMData") or {}).get("notam") or {}
        out.append({
            "id":        core.get("id") or "",
            "number":    core.get("number") or "",
            "type":      core.get("type") or "",
            "icao":      core.get("icaoLocation") or icao,
            "location":  core.get("location") or "",
            "effective_start": core.get("effectiveStart") or "",
            "effective_end":   core.get("effectiveEnd") or "",
            "classification":  core.get("classification") or "",
            "text":      core.get("text") or "",
        })
    return tuple(out)


def fetch_notams_for_icao(icao: str) -> list[dict]:
    """Return every active NOTAM for an ICAO airport identifier.

    Returns ``[]`` when credentials are missing (so the UI can show a
    configure-me notice without this raising).
    """
    if not icao:
        return []
    bust = str(int(time.time() // 600))  # 10-min bucket
    return list(_cached_fetch(icao.strip().upper(), bust))


def summarize_for_drill(icao: str) -> dict:
    """Compact summary for the drill panel.

    Returns::
        {
            "configured": bool,
            "icao": "KJFK",
            "count": 3,
            "equipment_out": 1,      # NOTAMs whose text mentions U/S, UNSERV, OUT OF SERVICE
            "asos_related": 1,       # NOTAMs whose text mentions ASOS / AWOS / WEATHER
            "items": [...]           # up to 5 most-recent
        }
    """
    icao_up = (icao or "").strip().upper()
    if not is_configured():
        return {"configured": False, "icao": icao_up, "count": 0,
                "equipment_out": 0, "asos_related": 0, "items": []}
    rows = fetch_notams_for_icao(icao_up)
    eq_hits = 0
    asos_hits = 0
    for r in rows:
        txt = (r.get("text") or "").upper()
        if any(tok in txt for tok in ("U/S", "UNSERV", "OUT OF SERVICE", "OTS")):
            eq_hits += 1
        if any(tok in txt for tok in ("ASOS", "AWOS", "WEATHER OBS", "WX OBS")):
            asos_hits += 1
    return {
        "configured": True,
        "icao": icao_up,
        "count": len(rows),
        "equipment_out": eq_hits,
        "asos_related": asos_hits,
        "items": rows[:5],
    }
