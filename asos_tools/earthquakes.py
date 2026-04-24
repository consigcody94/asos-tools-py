"""USGS Earthquake Hazards Program — real-time quake feed.

Public GeoJSON summary feeds at ``earthquake.usgs.gov`` — no auth, updated
every minute. We consume them to answer the ops question: *did a
recent quake plausibly knock this ASOS site offline?*

A M4+ within ~100 km or a M5+ within ~300 km of a station whose METAR just
went MISSING is a non-trivial correlation; AOMC can dispatch with prior
knowledge instead of chasing a failed comms link.

Feeds used:
  - ``summary/all_hour.geojson``   — everything M0+ in the last 60 minutes
  - ``summary/2.5_day.geojson``    — M2.5+ in the last 24 hours
  - ``summary/significant_week.geojson`` — PAGER-significant events, 7 days

Failure-mode: every function returns `[]` on network error; never raises.
"""

from __future__ import annotations

import logging
import math
import time
from functools import lru_cache
from typing import Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "fetch_recent_quakes",
    "quakes_near",
    "haversine_km",
    "USGS_FEEDS",
]

#: Public USGS GeoJSON summary endpoints. All zero-auth.
USGS_FEEDS = {
    "hour":          "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "day":           "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson",
    "day_all":       "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
    "week":          "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson",
    "significant":   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson",
}

_HEADERS = {
    "User-Agent": "owl.observation-watch-log/1.0 (github.com/consigcody94/asos-tools-py)",
    "Accept": "application/geo+json, application/json",
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two (lat, lon) pairs."""
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _process(feats: list) -> list[dict]:
    """Normalize USGS GeoJSON features → flat dicts we can use directly."""
    out: list[dict] = []
    for f in feats or []:
        props = f.get("properties") or {}
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        try:
            lon = float(coords[0])
            lat = float(coords[1])
            depth_km = float(coords[2]) if len(coords) >= 3 else None
        except (TypeError, ValueError):
            continue
        out.append({
            "id":       f.get("id") or props.get("code") or "",
            "mag":      props.get("mag"),
            "place":    props.get("place") or "",
            "time_ms":  props.get("time"),
            "updated":  props.get("updated"),
            "url":      props.get("url") or "",
            "tsunami":  bool(props.get("tsunami")),
            "alert":    props.get("alert") or "",
            "lat":      lat,
            "lon":      lon,
            "depth_km": depth_km,
        })
    return out


@lru_cache(maxsize=8)
def _cached_fetch(feed_key: str, bust: str) -> tuple:
    """Cached GET — ``bust`` is the 5-min time bucket. Returns tuple so it is
    hashable for ``lru_cache``."""
    url = USGS_FEEDS.get(feed_key)
    if not url:
        return ()
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12.0)
        if r.status_code != 200:
            logger.warning("usgs feed %s returned %s", feed_key, r.status_code)
            return ()
        data = r.json()
    except Exception:
        logger.exception("usgs feed %s failed", feed_key)
        return ()
    return tuple(_process(data.get("features") or []))


def fetch_recent_quakes(feed: str = "day") -> list[dict]:
    """Return the current contents of a USGS summary feed as a list of dicts.

    ``feed`` is one of ``USGS_FEEDS`` keys: ``hour``, ``day``, ``day_all``,
    ``week``, ``significant``. Results are cached for ~5 minutes.
    """
    # 5-min cache bucket — USGS publishes new records roughly every minute
    # but refetching every request is wasteful for the drill panel.
    bust = str(int(time.time() // 300))
    return list(_cached_fetch(feed, bust))


def quakes_near(
    lat: float,
    lon: float,
    *,
    radius_km: float = 300.0,
    min_mag: float = 2.5,
    feed: str = "day",
) -> list[dict]:
    """Return quakes within ``radius_km`` of ``(lat, lon)`` at ≥ ``min_mag``,
    sorted by distance ascending. Each record gets an added ``distance_km``.
    """
    try:
        qlat = float(lat)
        qlon = float(lon)
    except (TypeError, ValueError):
        return []
    rows = fetch_recent_quakes(feed)
    out: list[dict] = []
    for q in rows:
        m = q.get("mag")
        if m is None or m < min_mag:
            continue
        try:
            d = haversine_km(qlat, qlon, q["lat"], q["lon"])
        except Exception:
            continue
        if d <= radius_km:
            out.append({**q, "distance_km": round(d, 1)})
    out.sort(key=lambda r: r["distance_km"])
    return out
