"""NOAA National Hurricane Center — active tropical cyclone products.

``nhc.noaa.gov`` publishes a free JSON feed of every active tropical
system (Atlantic, East Pacific, Central Pacific). We surface:

- Current position + classification (TD / TS / HU / MH)
- Links to NHC's published cone / track / public-advisory graphics
- Per-station "storm is within ``radius_km`` of this site" flag

No auth, no key; cached 5 min server-side.

Off-season this returns an empty list. That's correct — the module gracefully
renders a "no active systems" footer rather than faking anything.
"""

from __future__ import annotations

import logging
import math
import time
from functools import lru_cache

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "fetch_active_storms",
    "stations_under_watch",
    "storm_classification_label",
    "NHC_FEED",
]

NHC_FEED = "https://www.nhc.noaa.gov/CurrentStorms.json"

_HEADERS = {
    "User-Agent": "owl.observation-watch-log/1.0 (github.com/consigcody94/asos-tools-py)",
    "Accept": "application/json",
}

#: ATCF classification short-codes → human label.
_CLASSIFICATION_LABELS = {
    "DB":  "Disturbance",
    "TD":  "Tropical Depression",
    "TS":  "Tropical Storm",
    "HU":  "Hurricane",
    "MH":  "Major Hurricane",
    "STD": "Subtropical Depression",
    "STS": "Subtropical Storm",
    "PTC": "Post-Tropical Cyclone",
    "EX":  "Extratropical",
    "LO":  "Low",
}


def storm_classification_label(code: str) -> str:
    if not code:
        return ""
    return _CLASSIFICATION_LABELS.get(code.upper(), code)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


@lru_cache(maxsize=1)
def _cached_fetch(bust: str) -> tuple:
    """``bust`` is the 5-min bucket key. Returns tuple for hashability."""
    try:
        r = requests.get(NHC_FEED, headers=_HEADERS, timeout=12.0)
        if r.status_code != 200:
            logger.warning("nhc current-storms returned %s", r.status_code)
            return ()
        data = r.json() or {}
    except Exception:
        logger.exception("nhc fetch failed")
        return ()
    storms = data.get("activeStorms") or []
    out: list[dict] = []
    for s in storms:
        try:
            lat = float(s.get("latitudeNumeric"))
            lon = float(s.get("longitudeNumeric"))
        except (TypeError, ValueError):
            continue
        out.append({
            "id":            s.get("id") or "",
            "bin":           s.get("binNumber") or "",
            "name":          s.get("name") or "",
            "classification": s.get("classification") or "",
            "class_label":   storm_classification_label(s.get("classification") or ""),
            "intensity_kt":  s.get("intensity") or "",
            "pressure_mb":   s.get("pressure") or "",
            "movement":      s.get("latestMovement") or "",
            "basin":         s.get("binNumber", "")[:2],
            "lat":           lat,
            "lon":           lon,
            "public_advisory":   (s.get("publicAdvisory") or {}).get("url", ""),
            "forecast_advisory": (s.get("forecastAdvisory") or {}).get("url", ""),
            "forecast_track":    (s.get("forecastTrack") or {}).get("zipFile", ""),
            "forecast_cone":     (s.get("forecastCone") or {}).get("zipFile", ""),
            "track_cone_graphic": (s.get("trackCone") or {}).get("url", ""),
            "wind_probabilities": (s.get("windProbabilities") or {}).get("zipFile", ""),
        })
    return tuple(out)


def fetch_active_storms() -> list[dict]:
    """Return all active NHC-tracked systems. Empty list outside hurricane
    season or during quiet periods. Cached ~5 min."""
    bust = str(int(time.time() // 300))
    return list(_cached_fetch(bust))


def stations_under_watch(
    lat: float,
    lon: float,
    storms: list[dict] | None = None,
    *,
    radius_km: float = 500.0,
) -> list[dict]:
    """Return every active storm within ``radius_km`` of ``(lat, lon)``,
    sorted by current distance ascending. Each record gets a ``distance_km``.

    ``radius_km=500`` gives a reasonable tropical-storm-force wind-radius
    outer envelope for a major hurricane; tighten to 300 km for more
    imminent-only filtering.
    """
    try:
        slat = float(lat)
        slon = float(lon)
    except (TypeError, ValueError):
        return []
    if storms is None:
        storms = fetch_active_storms()
    out: list[dict] = []
    for s in storms:
        try:
            d = _haversine_km(slat, slon, s["lat"], s["lon"])
        except Exception:
            continue
        if d <= radius_km:
            out.append({**s, "distance_km": round(d, 1)})
    out.sort(key=lambda r: r["distance_km"])
    return out
