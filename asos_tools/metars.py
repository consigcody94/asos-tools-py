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

import os
import re
import time
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
    "decode_maintenance_reasons",
    "MAINTENANCE_FLAG_CHAR",
    "SENSOR_INDICATORS",
]

# Sensor-down codes that appear in METAR remarks when a specific
# sensor or subsystem is fully offline. These produce hard "NO" data
# in the observation.
SENSOR_INDICATORS: dict[str, tuple[str, str]] = {
    "RVRNO":  ("RVR sensor",          "Runway Visual Range data not available"),
    "PWINO":  ("Precip ID sensor",    "Present weather identification not available"),
    "PNO":    ("Precip gauge",        "Precipitation amount not available"),
    "FZRANO": ("Freezing rain sensor","Freezing rain detection not available"),
    "TSNO":   ("Lightning sensor",    "Thunderstorm / lightning detection not available"),
    "SLPNO":  ("Pressure sensor",     "Sea-level pressure not available"),
}

# Location-qualified indicators (e.g. VISNO RWY06, CHINO RWY24L).
_LOCATION_INDICATORS: dict[str, tuple[str, str]] = {
    "VISNO": ("Visibility sensor",    "Visibility at {loc} not available"),
    "CHINO": ("Ceilometer",           "Cloud height at {loc} not available"),
}


def decode_maintenance_reasons(metar: str) -> list[dict]:
    """Decode why a METAR has the ``$`` maintenance flag.

    Inspects the remarks section for explicit sensor-down codes (RVRNO,
    PWINO, PNO, FZRANO, TSNO, VISNO, CHINO, SLPNO) and for missing data
    fields (M for temp/dew, missing altimeter, etc.).

    Returns a list of dicts, each with keys ``sensor`` and ``reason``.
    If the ``$`` is present but no specific indicator is found, a generic
    "ASOS internal tolerance check" entry is returned — this is by far
    the most common case (sensor drift, calibration age, component wear).

    Parameters
    ----------
    metar
        The raw METAR text string.

    Returns
    -------
    list of dict
        Each dict has ``{"sensor": str, "reason": str}``.
    """
    if not isinstance(metar, str) or not metar:
        return []

    upper = metar.upper()
    reasons: list[dict] = []

    # 1. Explicit sensor-down codes.
    for code, (sensor, desc) in SENSOR_INDICATORS.items():
        if code in upper:
            reasons.append({"sensor": sensor, "reason": desc})

    # 2. Location-qualified codes (VISNO RWY06, CHINO RWY24L).
    for code, (sensor, desc_template) in _LOCATION_INDICATORS.items():
        pattern = rf"{code}\s+(\S+)"
        m = re.search(pattern, upper)
        if m:
            loc = m.group(1).rstrip("$=").strip()
            reasons.append({
                "sensor": sensor,
                "reason": desc_template.format(loc=loc or "secondary location"),
            })
        elif code in upper:
            reasons.append({
                "sensor": sensor,
                "reason": desc_template.format(loc="secondary location"),
            })

    # 3. Missing data fields — clues from the observation body.
    # Temperature/dewpoint missing (M/M or M/...).
    if re.search(r"\bM/M\b", upper):
        reasons.append({
            "sensor": "Temp/Dew sensor",
            "reason": "Temperature and dewpoint both missing (M/M)",
        })
    # Altimeter missing.
    if " A////" in upper or " AM" in upper:
        reasons.append({
            "sensor": "Altimeter",
            "reason": "Altimeter setting missing",
        })
    # Wind missing.
    if "/////KT" in upper:
        reasons.append({
            "sensor": "Wind sensor",
            "reason": "Wind data missing",
        })

    # 4. If $ present but no specific indicator found — most common case.
    is_flagged = has_maintenance_flag(metar)
    if is_flagged and not reasons:
        reasons.append({
            "sensor": "Internal check",
            "reason": "ASOS self-test detected out-of-tolerance condition; "
                      "specific sensor not identified in METAR remarks",
        })

    return reasons


def decode_reasons_short(metar: str, wxcodes: str | None = None) -> str:
    """One-line summary of maintenance reasons, for table display.

    If ``wxcodes`` (parsed present weather from IEM) is provided and contains
    significant weather (TS, FZRA, SN, etc.), the summary is prefixed with
    the active weather so controllers see context alongside the sensor issue.
    """
    reasons = decode_maintenance_reasons(metar)
    if not reasons:
        return ""
    parts = [r["sensor"] for r in reasons]
    summary = " · ".join(parts)

    # Prefix with active weather if significant.
    if wxcodes and isinstance(wxcodes, str) and wxcodes.strip():
        wx = wxcodes.strip()
        # Flag severe/significant wx that makes a $ flag more operationally urgent.
        URGENT_WX = {"TS", "FZRA", "FZDZ", "SN", "PL", "GR", "FG", "BLSN", "+RA"}
        codes = set(wx.replace(",", " ").split())
        urgent = codes & URGENT_WX
        if urgent:
            summary = f"[{wx}] {summary}"
    return summary

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
    max_chunk: int = 100,
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

    # --------------------------------------------------------------
    # Primary source: AWC (Aviation Weather Center).  Unthrottled,
    # JSON, fast (~1.5s per 100-station chunk), federal-authoritative.
    # IEM becomes a fallback below when AWC is unavailable.
    # --------------------------------------------------------------
    if os.environ.get("OWL_METAR_SOURCE", "awc").lower() != "iem":
        try:
            from asos_tools.awc import fetch_metars_df as _awc_df
            # AWC uses full ICAO; normalize_station may have stripped K.
            icao_ids = ["K" + s if len(s) == 3 and s.isalpha() else s
                        for s in station_list]
            hours_back = max(1.0, (end - start).total_seconds() / 3600.0)
            df = _awc_df(icao_ids, hours_back=hours_back)
            if df is not None and not df.empty:
                return df
            import logging as _lg
            _lg.getLogger(__name__).warning(
                "AWC returned empty for %d stations; falling back to IEM",
                len(icao_ids),
            )
        except Exception:
            import logging as _lg
            _lg.getLogger(__name__).exception("AWC fetch raised; falling back to IEM")

    # Chunk large station lists to stay friendly with IEM's rate limiter.
    # Empirically IEM throttles hard on batch-style requests — even after
    # chunking to 200 we get 429s on a cold start.  Back off to 100/chunk
    # with 2s inter-chunk pause: 920 stations -> 10 chunks, ~25-35 s total,
    # which is well within IEM's tolerance.
    if max_chunk and len(station_list) > max_chunk:
        frames: list[pd.DataFrame] = []
        for i in range(0, len(station_list), max_chunk):
            sub = station_list[i : i + max_chunk]
            df_sub = fetch_metars(
                sub, start, end,
                timeout=timeout, session=session,
                max_chunk=0,  # prevent recursion
            )
            if df_sub is not None and not df_sub.empty:
                frames.append(df_sub)
            # Polite pause between chunks.  2s is conservative; if IEM
            # stops throttling this can come back down.
            if i + max_chunk < len(station_list):
                time.sleep(2.0)
        if not frames:
            # ---- NCEI fallback --------------------------------------
            # Every IEM chunk came back empty (rate-limited or 5xx).
            # Try NCEI as a federal-authoritative backup before giving
            # up entirely.  Slower but independent of IEM.
            try:
                from asos_tools.ncei import fetch_metars_ncei
                import logging as _lg
                _lg.getLogger(__name__).warning(
                    "IEM returned empty for all %d chunks; falling back to NCEI",
                    (len(station_list) + max_chunk - 1) // max_chunk,
                )
                ncei_df = fetch_metars_ncei(station_list, start, end)
                if ncei_df is not None and not ncei_df.empty:
                    return ncei_df.sort_values(
                        ["valid", "station"], kind="mergesort"
                    ).reset_index(drop=True)
            except Exception:  # noqa: BLE001
                pass
            return pd.DataFrame(columns=[
                "station", "valid", "metar", "has_maintenance"
            ])
        df = pd.concat(frames, ignore_index=True)
        return df.sort_values(["valid", "station"], kind="mergesort").reset_index(drop=True)

    params = {
        "station": ",".join(station_list),
        # Pull raw METAR + parsed fields we use for the decoder + watchlist.
        # wxcodes = present weather (RA, SN, TS, FG, etc.) parsed by IEM.
        # peak_wind_gust/drct = max gust from the METAR period.
        # vsby = visibility. skyc1/skyl1 = lowest cloud layer.
        "data": "metar,wxcodes,peak_wind_gust,peak_wind_drct,vsby,skyc1,skyl1,tmpf,dwpf,sknt,drct,alti",
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
    # Descriptive User-Agent — IEM prefers this over the default
    # "python-requests/..." which gets rate-limited more aggressively.
    sess.headers.update({
        "User-Agent": (
            "owl.observation-watch-log/1.0 "
            "(+github.com/consigcody94/asos-tools-py)"
        ),
        "Accept": "text/csv,text/plain,*/*",
    })

    # Retry transient 5xx AND 429 (rate limit) with backoff.
    # IEM routinely 503s under load; 429 means we (or another tenant on our
    # source IP, e.g. an HF Space / GH Actions runner) are hitting it too
    # fast.  Respect the Retry-After header if present, otherwise exponential
    # backoff.  429s need LONGER backoff than 5xx — the remote is signalling
    # deliberate throttling, not a transient crash.
    last_exc = None
    body = None
    for attempt in range(5):
        try:
            resp = sess.get(IEM_ENDPOINT_METAR, params=params, timeout=timeout)

            # Rate-limited.
            if resp.status_code == 429:
                last_exc = requests.HTTPError(
                    f"429 Too Many Requests from IEM", response=resp,
                )
                retry_after = resp.headers.get("Retry-After", "")
                try:
                    pause = float(retry_after) if retry_after else 0.0
                except ValueError:
                    pause = 0.0
                # Respect server guidance if sensible, else exponential.
                # Cap at 30 s so a very-long Retry-After doesn't hang the UI.
                pause = max(pause, 2.0 * (attempt + 1))
                pause = min(pause, 30.0)
                time.sleep(pause)
                continue

            # Server-side flakiness.
            if 500 <= resp.status_code < 600:
                last_exc = requests.HTTPError(
                    f"{resp.status_code} {resp.reason} from IEM",
                    response=resp,
                )
                time.sleep(0.8 * (attempt + 1))
                continue

            resp.raise_for_status()
            body = resp.text
            break
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            time.sleep(0.8 * (attempt + 1))
            continue
    if body is None:
        # All retries exhausted — return empty DataFrame rather than raising.
        # Upstream callers get a usable (if empty) result; logs capture why.
        import logging
        logging.getLogger(__name__).warning(
            "IEM unavailable after 3 retries: %s", last_exc,
        )
        return pd.DataFrame(columns=["station", "valid", "metar", "has_maintenance"])

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

    # Coerce parsed numeric fields (IEM sends "M" for missing).
    for col in ["tmpf", "dwpf", "sknt", "drct", "alti", "vsby",
                "peak_wind_gust", "peak_wind_drct", "skyl1"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["valid", "station"], kind="mergesort").reset_index(drop=True)
    # Column order — lead with the most useful fields.
    lead = ["station", "valid", "metar", "has_maintenance",
            "wxcodes", "tmpf", "dwpf", "sknt", "drct",
            "peak_wind_gust", "vsby", "skyc1", "alti"]
    lead = [c for c in lead if c in df.columns]
    extra = [c for c in df.columns if c not in lead]
    return df[lead + extra]
