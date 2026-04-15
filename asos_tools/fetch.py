"""Date-range queries for ASOS 1-minute surface observations.

Uses the Iowa Environmental Mesonet (IEM) ASOS 1-minute service, which ingests
NCEI's official 1-minute ASOS page-1 archive and exposes it as a queryable
CSV endpoint. The key property is *server-side* subsetting by date range: you
pay for only the minutes you ask for, across any number of stations, without
month-at-a-time downloads and without FTP.

Example
-------
>>> from datetime import datetime, timezone
>>> from asos_tools import fetch_1min
>>> t0 = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
>>> t1 = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)
>>> df = fetch_1min("KORD", t0, t1)
>>> df[["valid", "tmpf", "dwpf"]].head()

Notes
-----
NCEI's own Access Data Service v1 API does not currently expose the 1-minute
ASOS datasets (only aggregated products such as ``daily-summaries`` and
``global-hourly``), which is why this module queries IEM instead.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from typing import Iterable, Sequence, Union

import pandas as pd
import requests

__all__ = [
    "DEFAULT_VARS_1MIN",
    "IEM_ENDPOINT_1MIN",
    "fetch_1min",
    "normalize_station",
]

# Full list of 1-minute variables IEM exposes that are generally useful for
# surface meteorology. Consumers can pass a subset via the ``variables``
# keyword of :func:`fetch_1min`.
DEFAULT_VARS_1MIN: tuple[str, ...] = (
    "tmpf",       # air temperature, degF
    "dwpf",       # dewpoint, degF
    "sknt",       # 2-min mean wind speed, knots
    "drct",       # 2-min mean wind direction, degrees
    "gust_sknt",  # peak 1-min wind gust, knots
    "vis1_coeff", # primary visibility sensor extinction coefficient
    "vis1_nd",    # primary visibility sensor night/day flag
    "pres1",      # station pressure, inches Hg
    "precip",     # 1-min precipitation accumulation, inches
)

IEM_ENDPOINT_1MIN = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"


def normalize_station(station: str) -> str:
    """Normalize an ICAO-style station identifier for IEM's 1-minute service.

    IEM expects bare 3-letter FAA IDs for US airports (``ORD`` not ``KORD``),
    but full 4-letter codes for non-US (``PANC`` stays ``PANC``). This helper
    strips a leading ``K`` only for 4-character US identifiers and leaves
    everything else untouched.

    Parameters
    ----------
    station
        Input identifier, any case, may have surrounding whitespace.

    Returns
    -------
    str
        Station ID normalized for IEM.
    """
    s = station.strip().upper()
    if len(s) == 4 and s[0] == "K":
        return s[1:]
    return s


def _ensure_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _stations_as_list(stations: Union[str, Iterable[str]]) -> list[str]:
    if isinstance(stations, str):
        return [stations]
    return list(stations)


def fetch_1min(
    stations: Union[str, Iterable[str]],
    start: datetime,
    end: datetime,
    *,
    variables: Sequence[str] | None = None,
    timezone_label: str = "UTC",
    timeout: float = 120.0,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch ASOS 1-minute observations for a specific UTC date range.

    Parameters
    ----------
    stations
        One station identifier (e.g. ``"KORD"``) or an iterable of them.
        Leading ``K`` is stripped automatically for 4-character US stations.
    start, end
        ``datetime`` instances bounding the query. Naive datetimes are
        interpreted as UTC; aware datetimes are converted to UTC.
    variables
        Sequence of IEM variable names. Defaults to :data:`DEFAULT_VARS_1MIN`.
    timezone_label
        IANA timezone name passed to IEM's ``tz`` parameter. This controls
        what calendar IEM applies; the returned ``valid`` column is always a
        ``pd.Timestamp`` in UTC.
    timeout
        Seconds before the HTTP request is abandoned.
    session
        Optional :class:`requests.Session` to reuse connection pooling across
        calls. A new session is created if omitted.

    Returns
    -------
    pandas.DataFrame
        One row per station-minute in the requested window. Columns:
        ``station``, ``station_name``, ``valid`` (tz-aware UTC timestamp),
        plus one column per requested variable. Sorted by ``valid`` then
        ``station``.

    Raises
    ------
    ValueError
        If ``end`` is not strictly after ``start``, or if IEM refuses the
        request (e.g. unknown station, malformed parameters).
    requests.RequestException
        On transport-layer failures.
    """
    if end <= start:
        raise ValueError("end must be strictly after start")

    start_utc = _ensure_utc(start)
    end_utc = _ensure_utc(end)

    vars_list = list(variables) if variables is not None else list(DEFAULT_VARS_1MIN)
    station_list = [normalize_station(s) for s in _stations_as_list(stations)]

    params = {
        "station": ",".join(station_list),
        "vars": ",".join(vars_list),
        "sts": start_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "ets": end_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "sample": "1min",
        "what": "download",
        "delim": "comma",
        "tz": timezone_label,
    }

    sess = session or requests.Session()
    resp = sess.get(IEM_ENDPOINT_1MIN, params=params, timeout=timeout)
    resp.raise_for_status()

    body = resp.text
    # IEM returns HTTP 200 with a plain-text error message in the body for
    # bad input (unknown station, missing vars, etc.), so sniff the header.
    first_line = body.splitlines()[0] if body else ""
    if not first_line.startswith("station,"):
        raise ValueError(f"IEM rejected the request: {first_line!r}")

    # low_memory=False avoids pandas' chunked-inference DtypeWarning when a
    # column contains IEM's "M" missing-sentinel alongside real numbers; we
    # coerce to numeric further down regardless.
    df = pd.read_csv(StringIO(body), low_memory=False)
    if df.empty:
        return df

    # Parse 'valid(UTC)' -> tz-aware UTC Timestamp and rename to 'valid'.
    if "valid(UTC)" in df.columns:
        df["valid"] = pd.to_datetime(df["valid(UTC)"], utc=True)
        df = df.drop(columns=["valid(UTC)"])

    # Reorder: station, station_name, valid, then the rest in original order.
    lead = [c for c in ("station", "station_name", "valid") if c in df.columns]
    rest = [c for c in df.columns if c not in lead]
    df = df[lead + rest]

    # IEM encodes missing numeric readings as the sentinel "M" (and a few
    # other text values, e.g. visibility's "N"/"D" flag is already textual).
    # Coerce everything that should be a number to a true numeric dtype,
    # with NaN for anything unparseable.
    non_numeric = {"station", "station_name", "valid", "vis1_nd"}
    for col in df.columns:
        if col in non_numeric:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["valid", "station"], kind="mergesort").reset_index(drop=True)
    return df
