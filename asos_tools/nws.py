"""Live current-conditions from the NWS API (api.weather.gov).

Provides a lightweight, retry-hardened function to fetch the latest
observation for an ASOS station directly from the NWS — independent
of IEM. Useful for a "current weather" widget in the dashboard.

The NWS API is free, no auth, and rate-limited to ~5 req/s.
"""

from __future__ import annotations

import time
from typing import Any

import requests

__all__ = ["get_current_conditions", "NWS_API_BASE"]

NWS_API_BASE = "https://api.weather.gov"
_HEADERS = {
    "User-Agent": "(asos-tools-py, github.com/consigcody94/asos-tools-py)",
    "Accept": "application/geo+json",
}

# Fibonacci-backoff retry (inspired by paulokuong/noaa).
_FIB_DELAYS = [0.5, 0.5, 1, 1.5, 2.5, 4]


def _retry_get(url: str, *, timeout: float = 15, max_retries: int = 4) -> dict | None:
    """GET with fibonacci-backoff retry on 5xx or connection errors."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code >= 500:
                delay = _FIB_DELAYS[min(attempt, len(_FIB_DELAYS) - 1)]
                time.sleep(delay)
                continue
            # 4xx = bad request, don't retry
            return None
        except (requests.ConnectionError, requests.Timeout):
            delay = _FIB_DELAYS[min(attempt, len(_FIB_DELAYS) - 1)]
            time.sleep(delay)
    return None


def _safe(props: dict, key: str, fallback: Any = None) -> Any:
    """Extract a NWS observation value, handling their nested {value, unitCode} format."""
    val = props.get(key)
    if val is None:
        return fallback
    if isinstance(val, dict):
        v = val.get("value")
        return v if v is not None else fallback
    return val


def _c_to_f(c: float | None) -> float | None:
    return round(c * 9 / 5 + 32, 1) if c is not None else None


def _mps_to_kt(mps: float | None) -> float | None:
    return round(mps * 1.94384, 1) if mps is not None else None


def _m_to_mi(m: float | None) -> float | None:
    return round(m / 1609.344, 1) if m is not None else None


def _pa_to_inhg(pa: float | None) -> float | None:
    return round(pa / 3386.39, 2) if pa is not None else None


def get_current_conditions(station_id: str) -> dict | None:
    """Fetch the latest observation for an ASOS station from the NWS API.

    Parameters
    ----------
    station_id
        4-letter ICAO identifier (e.g. ``"KJFK"``).

    Returns
    -------
    dict or None
        Keys: ``station``, ``timestamp``, ``description``, ``temp_f``,
        ``dewpoint_f``, ``wind_speed_kt``, ``wind_direction``,
        ``wind_gust_kt``, ``visibility_mi``, ``pressure_inhg``,
        ``sky``, ``wx``, ``raw_metar``, ``icon_url``.
        Returns ``None`` if the station is unknown or the NWS API is down.
    """
    icao = station_id.strip().upper()
    url = f"{NWS_API_BASE}/stations/{icao}/observations/latest"
    data = _retry_get(url)
    if not data:
        return None

    props = data.get("properties", {})
    if not props:
        return None

    return {
        "station": icao,
        "timestamp": props.get("timestamp"),
        "description": props.get("textDescription", ""),
        "temp_f": _c_to_f(_safe(props, "temperature")),
        "dewpoint_f": _c_to_f(_safe(props, "dewpoint")),
        "humidity_pct": _safe(props, "relativeHumidity"),
        "wind_speed_kt": _mps_to_kt(_safe(props, "windSpeed")),
        "wind_direction": _safe(props, "windDirection"),
        "wind_gust_kt": _mps_to_kt(_safe(props, "windGust")),
        "visibility_mi": _m_to_mi(_safe(props, "visibility")),
        "pressure_inhg": _pa_to_inhg(_safe(props, "barometricPressure")),
        "sky": ", ".join(
            f"{c.get('amount', '?')} {c.get('base', {}).get('value', '?')}ft"
            for c in (props.get("cloudLayers") or [])
        ) or "CLR",
        "wx": ", ".join(
            (w.get("weather", "") or "") + " " + (w.get("rawString", "") or "")
            for w in (props.get("presentWeather") or [])
            if w
        ).strip() or "None",
        "raw_metar": props.get("rawMessage", ""),
        "icon_url": props.get("icon", ""),
        "heat_index_f": _c_to_f(_safe(props, "heatIndex")),
        "wind_chill_f": _c_to_f(_safe(props, "windChill")),
    }
