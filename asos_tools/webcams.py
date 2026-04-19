"""FAA WeatherCam client + public-portal deeplinks.

The FAA WeatherCams portal (``https://weathercams.faa.gov``) hosts 260+
FAA-owned cameras and ~530 hosted camera sites across CONUS, Alaska,
Hawaii, and Canada, refreshing every ~10 minutes. As of April 2026 the
JSON API endpoints return **401 Unauthorized** for unauthenticated
requests — the SPA uses a session cookie / token that isn't publicly
documented.

To keep O.W.L. useful without credentials, this module:

1. Attempts the JSON catalog fetch (populates if FAA ever re-opens it).
2. Gracefully returns an empty list on 401 — never raises.
3. Provides a **deeplink builder** that jumps directly to the public
   FAA WeatherCams page for any station, which IS publicly accessible
   and embeds the cameras in an iframe.

A future version can plug in alternative public sources (AOOS portal
for Alaska FAA cams, windy.com for international airports) — see the
``ALT_SOURCES`` registry below.
"""

from __future__ import annotations

import logging
import math
import os
import time
from functools import lru_cache
from typing import Iterable, Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "list_cameras",
    "cameras_near",
    "cameras_for_station",
    "latest_image_url",
    "portal_deeplink",
    "iframe_embed_url",
    "ALT_SOURCES",
    "Camera",
]


#: Future alternative camera sources (for reference; not yet wired).
ALT_SOURCES = [
    {"label": "AOOS Portal (Alaska FAA cams)", "url": "https://portal.aoos.org"},
    {"label": "Windy.com webcams",             "url": "https://www.windy.com/-Webcams/webcams?webcams"},
]


_BASE = os.environ.get("FAA_WEATHERCAMS_BASE", "https://weathercams.faa.gov")
_MANIFEST_PATH = "/api/Cameras"   # returns full catalog
_UA = "O.W.L./1.0 (+github.com/consigcody94/asos-tools-py)"


#: Simple dict schema — we keep it plain to avoid an extra dep.
#: Canonical fields: id, name, lat, lon, group, directions, image_url, updated.
Camera = dict


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    r_nm = 3440.065  # Earth radius in NM
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_nm * c


@lru_cache(maxsize=1)
def _cached_catalog(ts_bucket: int) -> list[Camera]:
    """Fetch the full FAA WeatherCam catalog. Cached per hour (ts_bucket)."""
    url = f"{_BASE.rstrip('/')}{_MANIFEST_PATH}"
    try:
        r = requests.get(url, timeout=20,
                         headers={"User-Agent": _UA, "Accept": "application/json"})
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception("FAA WeatherCam catalog fetch failed")
        return []

    # Normalize: the SPA returns a list of { Id, Name, Latitude, Longitude,
    # SiteNumber, IconaoCode, ... }. Field names vary by endpoint version.
    out: list[Camera] = []
    if isinstance(data, dict):
        data = data.get("cameras") or data.get("data") or []
    for c in data or []:
        try:
            cam_id = str(c.get("siteNumber") or c.get("Id") or c.get("id") or "")
            name = str(c.get("name") or c.get("Name") or "").strip()
            lat = float(c.get("latitude") or c.get("Latitude") or 0.0)
            lon = float(c.get("longitude") or c.get("Longitude") or 0.0)
            icao = str(c.get("icaoCode") or c.get("IcaoCode") or "").strip().upper()
            if not cam_id or (lat == 0.0 and lon == 0.0):
                continue
            out.append({
                "id": cam_id,
                "name": name,
                "lat": lat,
                "lon": lon,
                "icao": icao or None,
                "image_url": (
                    c.get("imageUrl")
                    or c.get("ImageUrl")
                    or f"{_BASE}/api/CameraImage/{cam_id}"
                ),
                "clear_day_url": (
                    c.get("clearDayUrl")
                    or c.get("ClearDayUrl")
                    or None
                ),
                "updated": c.get("lastUpdated") or c.get("LastUpdated") or None,
            })
        except Exception:
            logger.warning("Skipping malformed camera entry: %s", c)
    return out


def list_cameras() -> list[Camera]:
    """Full FAA WeatherCam catalog."""
    return _cached_catalog(int(time.time()) // 3600)   # 1-hour bucket


def cameras_near(lat: float, lon: float, *,
                 radius_nm: float = 25.0,
                 limit: int = 4) -> list[Camera]:
    """Return cameras within ``radius_nm`` of a point, nearest first."""
    cats = list_cameras()
    scored = []
    for c in cats:
        try:
            d = _haversine_nm(lat, lon, c["lat"], c["lon"])
            if d <= radius_nm:
                scored.append((d, c))
        except Exception:
            continue
    scored.sort(key=lambda t: t[0])
    return [dict(c, distance_nm=round(d, 1)) for d, c in scored[:limit]]


def cameras_for_station(station: dict, *,
                        radius_nm: float = 25.0,
                        limit: int = 4) -> list[Camera]:
    """Convenience wrapper: ``station`` dict with lat/lon keys."""
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        return []
    return cameras_near(float(lat), float(lon),
                        radius_nm=radius_nm, limit=limit)


def latest_image_url(cam_id: str) -> str:
    """Direct URL to the most recent camera image (updates every ~10 min)."""
    return f"{_BASE.rstrip('/')}/api/CameraImage/{cam_id}"


def portal_deeplink(station: dict) -> str:
    """Return a public FAA WeatherCams portal URL centered on the station.

    This works today without auth — opens in a new tab and shows all
    cameras within ~25 NM of the given lat/lon.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        return f"{_BASE.rstrip('/')}/cameras"
    # The public portal accepts /map/lon,lat,lon,lat bbox queries.
    lat_f = float(lat); lon_f = float(lon)
    bbox = f"{lon_f - 0.6:.4f},{lat_f - 0.4:.4f},{lon_f + 0.6:.4f},{lat_f + 0.4:.4f}"
    return f"{_BASE.rstrip('/')}/map/{bbox}"


def iframe_embed_url(station: dict) -> str:
    """Return a URL suitable for embedding in an iframe (portal view)."""
    return portal_deeplink(station)
