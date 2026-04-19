"""Aviation Weather Center (AWC) API client.

The AWC public data API (``https://aviationweather.gov/api/data``) exposes
METAR, TAF, SIGMET, AIRMET, PIREP, and AFD products as JSON. No auth,
no key, free, federal-authoritative.

All functions are network-safe:

- Explicit timeouts (15 s).
- Returns empty lists on failure (never raises).
- Short-circuit with in-process LRU cache.

Usage::

    from asos_tools.awc import (
        fetch_metar, fetch_taf, fetch_airsigmet,
        fetch_pirep, flight_category,
    )
    metars = fetch_metar(["KJFK", "KLGA"])
    tafs = fetch_taf(["KJFK"])
    sigmets = fetch_airsigmet()
    pireps = fetch_pirep(age_hours=2)
"""

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache
from typing import Iterable, Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "fetch_metar",
    "fetch_metars_df",
    "fetch_taf",
    "fetch_airsigmet",
    "fetch_pirep",
    "fetch_afd",
    "flight_category",
    "FLIGHT_CATEGORIES",
]

#: API base; overridable via env var for testing / mirrors.
_BASE = os.environ.get("AWC_API_BASE", "https://aviationweather.gov/api/data")

#: Ordered by severity (VFR best, LIFR worst).
FLIGHT_CATEGORIES = ("VFR", "MVFR", "IFR", "LIFR")


def _get(path: str, params: dict, *, timeout: float = 15.0) -> list | dict | None:
    """GET ``{_BASE}/{path}`` and return JSON (list/dict). None on failure."""
    url = f"{_BASE.rstrip('/')}/{path.lstrip('/')}"
    try:
        r = requests.get(url, params=params, timeout=timeout,
                         headers={"User-Agent": "O.W.L./1.0 (+github.com/consigcody94/asos-tools-py)"})
        r.raise_for_status()
        # AWC returns JSON even on empty result; will be [] or {}.
        return r.json()
    except Exception:
        logger.exception("AWC %s failed (url=%s)", path, url)
        return None


def _chunks(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def fetch_metar(ids: Iterable[str], *, hours_back: float = 0,
                format_: str = "json") -> list[dict]:
    """Return the latest METAR(s) for each ICAO id.

    ``hours_back`` = 0 returns only the most recent METAR per station.
    AWC caps ``ids`` at ~25 per request; we chunk automatically.
    """
    ids_list = [str(i).strip().upper() for i in ids if str(i).strip()]
    if not ids_list:
        return []
    out: list[dict] = []
    for chunk in _chunks(ids_list, 25):
        params = {
            "ids": ",".join(chunk),
            "format": format_,
            "taf": "false",
        }
        if hours_back:
            params["hours"] = str(float(hours_back))
        data = _get("metar", params)
        if isinstance(data, list):
            out.extend(data)
    return out


def fetch_metars_df(
    ids: Iterable[str],
    *,
    hours_back: float = 4.0,
    chunk_size: int = 100,
    pause_s: float = 0.25,
):
    """Return a :class:`pandas.DataFrame` of METARs, shape-compatible with
    :func:`asos_tools.metars.fetch_metars`.

    Preferred over the IEM path when IEM is rate-limiting us — AWC is
    faster (~1.5 s per 100-station chunk), unthrottled in our experience,
    and returns richer JSON (decoded clouds, flight category, lat/lon).

    Columns returned:

        station          - 3- or 4-letter ICAO (K stripped to match IEM)
        valid            - tz-aware UTC pandas.Timestamp of the observation
        metar            - raw METAR string (rawOb)
        has_maintenance  - True iff rawOb contains the '$' flag
        wxcodes          - present weather (space-joined string)
        tmpf, dwpf       - temperature / dewpoint in degrees F
        sknt, drct       - wind speed (kt) / direction (deg)
        alti             - altimeter (inHg)
        vsby             - visibility (statute miles)
        peak_wind_gust   - wind gust (kt) — AWC field ``wgst``
        skyc1            - lowest cloud cover code
        skyl1            - lowest cloud base (ft)
        flt_cat          - VFR/MVFR/IFR/LIFR (AWC field ``fltCat``)
        lat, lon         - from AWC (useful for downstream geo joins)

    Returns an empty DataFrame on any failure (logs, never raises).
    """
    import pandas as pd

    ids_list = [str(i).strip().upper() for i in ids if str(i).strip()]
    empty_cols = [
        "station", "valid", "metar", "has_maintenance",
        "wxcodes", "tmpf", "dwpf", "sknt", "drct", "alti",
        "vsby", "peak_wind_gust", "skyc1", "skyl1",
        "flt_cat", "lat", "lon",
    ]
    if not ids_list:
        return pd.DataFrame(columns=empty_cols)

    def _c_to_f(c):
        try:
            return float(c) * 9.0 / 5.0 + 32.0
        except Exception:
            return None

    def _mb_to_inhg(mb):
        try:
            return float(mb) / 33.8639
        except Exception:
            return None

    def _visib_num(v):
        """AWC returns visibility as float, "10+", or "1/2" sometimes."""
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s.endswith("+"):
            return float(s[:-1])
        try:
            return float(s)
        except Exception:
            return None

    all_rows: list[dict] = []
    for chunk in _chunks(ids_list, chunk_size):
        params = {
            "ids": ",".join(chunk),
            "format": "json",
            "hours": str(float(hours_back)),
            "taf": "false",
        }
        data = _get("metar", params, timeout=30)
        if not isinstance(data, list):
            continue
        for rec in data:
            raw = rec.get("rawOb") or ""
            obs_ts = rec.get("obsTime")
            if obs_ts is None:
                continue
            try:
                valid = pd.to_datetime(int(obs_ts), unit="s", utc=True)
            except Exception:
                continue
            icao = (rec.get("icaoId") or "").strip().upper()
            # Match IEM convention: drop leading K for 4-letter ICAOs.
            station = icao[1:] if len(icao) == 4 and icao.startswith("K") else icao
            ceiling_ft = None
            skyc1 = None
            for cl in rec.get("clouds") or []:
                cov = (cl.get("cover") or "").upper()
                if cov in {"BKN", "OVC", "OVX"}:
                    ceiling_ft = cl.get("base")
                    skyc1 = cov
                    break
                if skyc1 is None:
                    skyc1 = cov
            fc = rec.get("fltCat") or flight_category(
                _visib_num(rec.get("visib")), ceiling_ft)
            all_rows.append({
                "station": station,
                "valid": valid,
                "metar": raw,
                "has_maintenance": "$" in raw,
                "wxcodes": rec.get("wxString") or "",
                "tmpf": _c_to_f(rec.get("temp")),
                "dwpf": _c_to_f(rec.get("dewp")),
                "sknt": rec.get("wspd"),
                "drct": rec.get("wdir") if isinstance(rec.get("wdir"), (int, float)) else None,
                "alti": _mb_to_inhg(rec.get("altim")),
                "vsby": _visib_num(rec.get("visib")),
                "peak_wind_gust": rec.get("wgst"),
                "skyc1": skyc1,
                "skyl1": ceiling_ft,
                "flt_cat": fc,
                "lat": rec.get("lat"),
                "lon": rec.get("lon"),
            })
        if pause_s and chunk is not ids_list[-len(chunk):]:
            time.sleep(pause_s)

    if not all_rows:
        return pd.DataFrame(columns=empty_cols)
    df = pd.DataFrame(all_rows)
    return df.sort_values(
        ["valid", "station"], kind="mergesort"
    ).reset_index(drop=True)


def fetch_taf(ids: Iterable[str], *, format_: str = "json") -> list[dict]:
    """Return decoded TAF(s) for each ICAO id."""
    ids_list = [str(i).strip().upper() for i in ids if str(i).strip()]
    if not ids_list:
        return []
    out: list[dict] = []
    for chunk in _chunks(ids_list, 25):
        data = _get("taf", {"ids": ",".join(chunk), "format": format_})
        if isinstance(data, list):
            out.extend(data)
    return out


def fetch_airsigmet(*, format_: str = "json",
                    type_: Optional[str] = None) -> list[dict]:
    """Return active SIGMETs + AIRMETs.

    ``type_`` ∈ {'airmet', 'sigmet', 'outlook'} narrows the response.
    """
    params: dict = {"format": format_}
    if type_:
        params["type"] = type_
    data = _get("airsigmet", params)
    return data if isinstance(data, list) else []


#: Default CONUS bounding box (AWC PIREP requires bbox OR station+radius).
CONUS_BBOX = "24.5,-125,49.5,-66"


def fetch_pirep(*, age_hours: float = 2, format_: str = "json",
                bbox: Optional[str] = CONUS_BBOX,
                station: Optional[str] = None,
                distance_nm: int = 200) -> list[dict]:
    """Return PIREPs (pilot weather reports) in the given window.

    Must specify either ``bbox`` OR ``station + distance_nm``. Defaults
    to CONUS bbox if neither given.
    """
    params: dict = {"format": format_, "age": str(int(age_hours))}
    if station:
        params["id"] = str(station).upper().strip()
        params["distance"] = str(int(distance_nm))
    elif bbox:
        params["bbox"] = bbox
    data = _get("pirep", params)
    if isinstance(data, list):
        return data
    # raw format returns plain text; split into line-records
    if isinstance(data, str) and data.strip():
        return [{"raw": line.strip()} for line in data.splitlines() if line.strip()]
    return []


def fetch_afd(*, cwa: Optional[str] = None,
              format_: str = "json") -> list[dict]:
    """Return Area Forecast Discussions (optionally narrowed by CWA code)."""
    params: dict = {"format": format_}
    if cwa:
        params["cwa"] = cwa
    data = _get("afd", params)
    return data if isinstance(data, list) else []


def flight_category(
    visibility_sm: Optional[float],
    ceiling_ft: Optional[float],
) -> str:
    """Derive VFR / MVFR / IFR / LIFR from visibility + ceiling.

    FAA thresholds:
        LIFR : vis <1 SM     OR ceiling <500 ft
        IFR  : vis <3 SM     OR ceiling <1000 ft
        MVFR : vis <=5 SM    OR ceiling <=3000 ft
        VFR  : otherwise
    Missing data is treated as the better of the two.
    """
    v = visibility_sm if visibility_sm is not None else 99
    c = ceiling_ft if ceiling_ft is not None else 99_000
    if v < 1 or c < 500:
        return "LIFR"
    if v < 3 or c < 1000:
        return "IFR"
    if v <= 5 or c <= 3000:
        return "MVFR"
    return "VFR"
