"""Fetch raw METAR/SPECI reports from IEM and flag maintenance-indicator rows.

A trailing ``$`` on a METAR is the ASOS **maintenance check indicator** —
the station has flagged itself as needing maintenance, typically because a
sensor is degraded or a component has exceeded tolerance. Data from a report
with the ``$`` flag should be treated with extra skepticism.

This module pairs :func:`fetch_metars` with detection helpers so you can
compare "clean" vs "flagged" observations for any station/window.

Example
-------
>>> from asos_tools.metars import fetch_metars
>>> df = fetch_metars("KJFK", t0, t1)
>>> df[["valid", "has_maintenance"]].head()
>>> flagged_rate = df["has_maintenance"].mean()
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from typing import Iterable, Union

import pandas as pd
import requests

from asos_tools.fetch import normalize_station  # re-use K-prefix logic

__all__ = [
    "IEM_ENDPOINT_METAR",
    "fetch_metars",
    "has_maintenance_flag",
    "MAINTENANCE_FLAG_CHAR",
]

IEM_ENDPOINT_METAR = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
MAINTENANCE_FLAG_CHAR = "$"


def has_maintenance_flag(metar: str | float | None) -> bool:
    """Return True if a METAR string ends with the ``$`` maintenance flag."""
    if not isinstance(metar, str) or not metar:
        return False
    # Reports may end with optional `=` terminator; strip it before checking.
    s = metar.rstrip().rstrip("=").rstrip()
    return s.endswith(MAINTENANCE_FLAG_CHAR)


def _ensure_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _stations_as_list(stations: Union[str, Iterable[str]]) -> list[str]:
    return [stations] if isinstance(stations, str) else list(stations)


def fetch_metars(
    stations: Union[str, Iterable[str]],
    start: datetime,
    end: datetime,
    *,
    timeout: float = 120.0,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch raw METAR/SPECI reports for a UTC date range.

    Parameters
    ----------
    stations
        One ICAO-style station identifier or an iterable of them. Leading
        ``K`` is stripped automatically for 4-character US stations.
    start, end
        Bounding ``datetime`` objects; naive inputs are interpreted as UTC.
    timeout, session
        Passed through to :mod:`requests`.

    Returns
    -------
    pandas.DataFrame
        Columns:

        * ``station`` — 3- or 4-letter identifier
        * ``valid`` — tz-aware UTC :class:`pandas.Timestamp`
        * ``metar`` — raw METAR text as reported
        * ``has_maintenance`` — boolean; True iff ``metar`` ends with ``$``

        Sorted ascending by ``valid`` then ``station``.
    """
    if end <= start:
        raise ValueError("end must be strictly after start")

    start_utc = _ensure_utc(start)
    end_utc = _ensure_utc(end)

    station_list = [normalize_station(s) for s in _stations_as_list(stations)]

    params = {
        "station": ",".join(station_list),
        "data": "metar",
        "year1": start_utc.year, "month1": start_utc.month,
        "day1": start_utc.day, "hour1": start_utc.hour,
        "minute1": start_utc.minute,
        "year2": end_utc.year, "month2": end_utc.month,
        "day2": end_utc.day, "hour2": end_utc.hour,
        "minute2": end_utc.minute,
        "tz": "Etc/UTC",
        "format": "onlycomma",
        "latlon": "no",
        "elev": "no",
        "missing": "M",
        "trace": "T",
        "direct": "no",
        "report_type": 3,   # 3 == MADIS routine + special
    }

    sess = session or requests.Session()
    resp = sess.get(IEM_ENDPOINT_METAR, params=params, timeout=timeout)
    resp.raise_for_status()

    body = resp.text
    if not body:
        return pd.DataFrame(columns=["station", "valid", "metar", "has_maintenance"])

    first_line = body.splitlines()[0]
    if not first_line.startswith("station,"):
        raise ValueError(f"IEM rejected the request: {first_line!r}")

    df = pd.read_csv(StringIO(body), low_memory=False)
    if df.empty:
        df["has_maintenance"] = pd.Series(dtype=bool)
        return df

    # Parse the 'valid' column to UTC.
    df["valid"] = pd.to_datetime(df["valid"], utc=True, errors="coerce")
    df = df.dropna(subset=["valid"])

    # Maintenance flag.
    df["has_maintenance"] = df["metar"].fillna("").map(has_maintenance_flag)

    df = df.sort_values(["valid", "station"], kind="mergesort").reset_index(drop=True)
    # Column order.
    cols = ["station", "valid", "metar", "has_maintenance"]
    extra = [c for c in df.columns if c not in cols]
    return df[cols + extra]
