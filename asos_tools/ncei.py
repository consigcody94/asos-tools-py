"""NCEI (National Centers for Environmental Information) direct fallback.

When IEM is down or rate-limiting us, we pull the same raw METAR data
directly from NCEI. This is slower than IEM (authoritative federal archive
vs community mirror) but gives us an independent source of truth.

Public endpoint: ``https://www.ncei.noaa.gov/access/services/data/v1``
- No auth, free, federal authoritative.
- Returns JSON/CSV/XML.

Usage::

    from asos_tools.ncei import fetch_metars_ncei
    from datetime import datetime, timezone, timedelta
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=6)
    df = fetch_metars_ncei(["KJFK", "KLGA"], start, end)
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from typing import Iterable, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

__all__ = ["fetch_metars_ncei", "service_available"]


_BASE = os.environ.get("NCEI_API_BASE",
                       "https://www.ncei.noaa.gov/access/services/data/v1")
_UA = "O.W.L./1.0 (+github.com/consigcody94/asos-tools-py)"


def service_available(timeout: float = 5.0) -> bool:
    """Cheap HEAD ping to see if NCEI is reachable."""
    try:
        r = requests.head(_BASE, timeout=timeout,
                          headers={"User-Agent": _UA})
        return r.status_code < 500
    except Exception:
        return False


def _ncei_station_id(icao: str) -> str:
    """NCEI uses WBAN/USAF composite IDs; ICAO is acceptable for most ASOS."""
    # For ASOS stations, NCEI accepts "K" + 3-letter ICAO directly via the
    # global-hourly dataset. E.g. "KJFK" -> "72503014732" (USAF-WBAN) but
    # the ICAO form works for recent observations on the v1 service.
    return icao.strip().upper()


def fetch_metars_ncei(
    station_ids: Iterable[str],
    start: datetime,
    end: datetime,
    *,
    timeout: float = 45.0,
) -> pd.DataFrame:
    """Pull METARs from NCEI's data access service.

    Returns a DataFrame with columns compatible with ``watchlist.build_watchlist``:
    ``station, valid, metar, has_maintenance``.

    Returns an empty DataFrame on any failure (logs the exception).
    """
    ids = [_ncei_station_id(s) for s in station_ids if s]
    if not ids:
        return pd.DataFrame(columns=["station", "valid", "metar", "has_maintenance"])

    # NCEI's v1 service expects YYYY-MM-DDTHH:MM:SS without TZ and in UTC
    def _fmt(t: datetime) -> str:
        return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "dataset": "global-hourly",
        "stations": ",".join(ids),
        "startDate": _fmt(start),
        "endDate": _fmt(end),
        "dataTypes": "MET",
        "format": "json",
        "boundingBox": "90,-180,-90,180",
        "units": "metric",
    }
    try:
        r = requests.get(_BASE, params=params, timeout=timeout,
                         headers={"User-Agent": _UA})
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception("NCEI fetch failed")
        return pd.DataFrame(columns=["station", "valid", "metar", "has_maintenance"])

    if not data:
        return pd.DataFrame(columns=["station", "valid", "metar", "has_maintenance"])

    # NCEI returns a list of hourly records per station.
    rows = []
    for rec in data:
        call = rec.get("CALL_SIGN") or rec.get("STATION") or ""
        date = rec.get("DATE") or ""
        metar = rec.get("REM") or rec.get("REPORT_TYPE") or ""
        try:
            ts = pd.to_datetime(date, utc=True, errors="coerce")
        except Exception:
            ts = pd.NaT
        has_maint = "$" in (metar or "")
        rows.append({
            "station": (call or "").strip().upper().lstrip("K") or "",
            "valid": ts,
            "metar": metar,
            "has_maintenance": has_maint,
        })
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["valid"])
    return df
