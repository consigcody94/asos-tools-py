"""FAA WeatherCams client — public API, no auth, browser-headers only.

The FAA WeatherCams portal at ``https://weathercams.faa.gov`` hosts ~926
camera sites across CONUS, Alaska, Hawaii, and select international
locations.  Each *site* may contain 1-4 *cameras* pointing in different
compass directions.  Images refresh every ~10 minutes.

The portal is a React SPA whose `/api/*` endpoints originally returned
**401 Unauthorized** to direct curl/python-requests calls.  In practice
the API gates on browser-style headers (``User-Agent`` and ``Referer``);
sending those clears the 401 and the API responds normally.

Endpoint surface (reverse-engineered from the production JS bundle —
``apiEndpoint:`` config strings):

================================================  ============================
``GET /api/sites?zoom=4``                         List all 926 camera sites.
``GET /api/sites/{siteId}``                       Single-site detail.
``GET /api/cameras/{cameraId}``                   Camera metadata.
``GET /api/cameras/{cameraId}/images``            Last 5 images + CDN URIs.
``GET /api/airports``                             FAA airport metadata.
``GET /api/aircraft-reports``                     PIREPs.
``GET /api/airsigmets``                           AIRMETs / SIGMETs.
``GET /api/tafs/stations``                        TAFs.
``GET /api/metars/stations``                      METARs.
``GET /api/tfrs``                                 Temporary flight restrictions.
================================================  ============================

Image CDN: ``https://images.wcams-static.faa.gov/webimages/{siteId}/{day}/...``
serves directly without any header gating.

Schema
------
Site (returned by ``/api/sites``):

    {
        "siteId":      1024,
        "siteName":    "Fort Abercrombie",
        "siteArea":    "Kodiak Island",
        "latitude":    57.837074,
        "longitude":  -152.35168,
        "country":     "US",
        "icao":        None,                       # often null for non-airport sites
        "operatedBy":  "Kingfisher Air",
        "siteActive":  True,
        "cameras":     [Camera, ...],
    }

Camera:

    {
        "cameraId":            13804,
        "cameraName":          "Camera 2",
        "cameraDirection":     "NorthEast",
        "cameraBearing":       30,
        "cameraInMaintenance": False,
        "cameraOutOfOrder":    False,
        "cameraLastSuccess":   "2026-04-19T05:48:20.175Z",
        "siteId":              1024,
    }

Image (from ``/api/cameras/{id}/images``):

    {
        "cameraId":      13804,
        "imageFilename": "13804-1776578365717.jpg",
        "imageUri":      "https://images.wcams-static.faa.gov/webimages/1024/19/13804-...jpg",
        "imageDatetime": "2026-04-19T05:59:48.548Z",
    }

The functions below return *normalized* dicts using the keys ``id, name,
lat, lon, icao, image_url, updated, distance_nm`` so call sites don't
have to know FAA's verbose field naming.
"""

from __future__ import annotations

import logging
import math
import os
import time
from functools import lru_cache
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "list_sites",
    "list_cameras",
    "cameras_near",
    "cameras_for_station",
    "latest_image_url",
    "site_images",
    "portal_deeplink",
    "iframe_embed_url",
    "Camera",
    "Site",
]


_BASE = os.environ.get("FAA_WEATHERCAMS_BASE", "https://weathercams.faa.gov")
_CDN_BASE = "https://images.wcams-static.faa.gov"

# Browser headers — the API gates on these. UA can be any modern browser
# string; Referer must be the SPA origin.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://weathercams.faa.gov/",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Network defaults — the sites payload is ~2.6 MB so allow a generous
# read timeout. Connection timeout stays tight to fail fast on outages.
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 30.0

Site = dict
Camera = dict


# ---------------------------------------------------------------------------
# Distance helper
# ---------------------------------------------------------------------------
def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    r_nm = 3440.065  # Earth radius in NM
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_nm * c


# ---------------------------------------------------------------------------
# Low-level fetch — cached per hour
# ---------------------------------------------------------------------------
def _get_json(path: str, params: Optional[dict] = None) -> Any:
    """Authenticated GET against the FAA WeatherCams API.

    Returns the parsed JSON ``payload`` on success, ``None`` on any
    error. Never raises — webcams are a nice-to-have, not critical
    infrastructure, and the rest of the app must keep running if the
    portal is down.
    """
    url = f"{_BASE.rstrip('/')}{path}"
    try:
        r = requests.get(
            url, params=params, headers=_HEADERS,
            timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT),
        )
        if r.status_code != 200:
            logger.warning("FAA %s -> HTTP %s", path, r.status_code)
            return None
        body = r.json()
        if isinstance(body, dict) and not body.get("success", True):
            logger.warning("FAA %s success=false: %s", path, body.get("error"))
            return None
        return body.get("payload") if isinstance(body, dict) else body
    except Exception:  # noqa: BLE001
        logger.exception("FAA fetch failed: %s", url)
        return None


# ---------------------------------------------------------------------------
# /api/sites — full catalog
# ---------------------------------------------------------------------------
@lru_cache(maxsize=4)
def _cached_sites(hour_bucket: int) -> list[Site]:
    """Fetch and normalize the full site catalog. Cache 1 hour per bucket."""
    raw = _get_json("/api/sites", params={"zoom": 4})
    if not raw:
        return []
    out: list[Site] = []
    for s in raw:
        try:
            site_id = int(s["siteId"])
        except Exception:
            continue
        cams: list[Camera] = []
        for c in s.get("cameras") or []:
            try:
                cam_id = int(c["cameraId"])
            except Exception:
                continue
            cams.append({
                "id": cam_id,
                "name": c.get("cameraName") or "",
                "direction": c.get("cameraDirection"),
                "bearing": c.get("cameraBearing"),
                "in_maintenance": bool(c.get("cameraInMaintenance")),
                "out_of_order": bool(c.get("cameraOutOfOrder")),
                "last_success": c.get("cameraLastSuccess"),
                "site_id": site_id,
            })
        out.append({
            "id": site_id,
            "name": (s.get("siteName") or "").strip(),
            "area": (s.get("siteArea") or "").strip(),
            "lat": float(s.get("latitude") or 0.0),
            "lon": float(s.get("longitude") or 0.0),
            "country": s.get("country"),
            "icao": (s.get("icao") or "").strip().upper() or None,
            "operated_by": s.get("operatedBy"),
            "active": bool(s.get("siteActive", True)),
            "cameras": cams,
        })
    return out


def list_sites() -> list[Site]:
    """Return all FAA WeatherCam sites (cached 1 hour)."""
    return _cached_sites(int(time.time()) // 3600)


def list_cameras() -> list[Camera]:
    """Flatten all cameras across all sites — back-compat with v1 API."""
    out: list[Camera] = []
    for site in list_sites():
        for cam in site.get("cameras", []):
            out.append({**cam,
                        "lat": site["lat"],
                        "lon": site["lon"],
                        "site_name": site["name"],
                        "icao": site.get("icao")})
    return out


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------
def cameras_near(lat: float, lon: float, *,
                 radius_nm: float = 25.0,
                 limit: int = 4) -> list[Camera]:
    """Cameras within ``radius_nm`` of a point, nearest first.

    Returns one entry per camera (not per site), so a site with 4
    cameras at the same lat/lon yields 4 entries. Each entry is
    enriched with ``distance_nm`` (rounded to 1 decimal).
    """
    out = []
    for cam in list_cameras():
        try:
            d = _haversine_nm(lat, lon, cam["lat"], cam["lon"])
        except Exception:
            continue
        if d <= radius_nm and not cam.get("out_of_order"):
            out.append({**cam, "distance_nm": round(d, 1)})
    out.sort(key=lambda c: c["distance_nm"])
    return out[:limit]


def cameras_for_station(station: dict, *,
                        radius_nm: float = 25.0,
                        limit: int = 4) -> list[Camera]:
    """ASOS-station convenience wrapper: pass a station dict with lat/lon."""
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        return []
    return cameras_near(float(lat), float(lon),
                        radius_nm=radius_nm, limit=limit)


# ---------------------------------------------------------------------------
# Per-camera image URLs
# ---------------------------------------------------------------------------
@lru_cache(maxsize=512)
def _cached_camera_images(cam_id: int, minute_bucket: int) -> list[dict]:
    """Last few images for one camera. Cached 5 minutes per bucket.

    Returns a list of ``{filename, url, datetime}`` dicts, newest first.
    Empty list on any failure.
    """
    raw = _get_json(f"/api/cameras/{cam_id}/images")
    if not raw:
        return []
    return [
        {
            "filename": img.get("imageFilename"),
            "url": img.get("imageUri"),
            "datetime": img.get("imageDatetime"),
        }
        for img in raw
        if img.get("imageUri")
    ]


def site_images(cam_id: int) -> list[dict]:
    """Return the last few image entries for a single camera."""
    return _cached_camera_images(int(cam_id), int(time.time()) // 300)


def latest_image_url(cam_id: int) -> Optional[str]:
    """Direct CDN URL of the most recent image for ``cam_id``.

    Returns ``None`` if the camera has no recent images or the API is
    unreachable. The returned URL serves with no auth headers required
    (CDN is unauthenticated), so it can be used as an ``<img src>``
    directly in client-side HTML.
    """
    imgs = site_images(cam_id)
    return imgs[0]["url"] if imgs else None


# ---------------------------------------------------------------------------
# Public-portal deep links
# ---------------------------------------------------------------------------
def portal_deeplink(station: dict) -> str:
    """Public FAA WeatherCams portal URL centered on the station's bbox."""
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        return f"{_BASE.rstrip('/')}/cameras"
    lat_f = float(lat)
    lon_f = float(lon)
    bbox = (
        f"{lon_f - 0.6:.4f},{lat_f - 0.4:.4f},"
        f"{lon_f + 0.6:.4f},{lat_f + 0.4:.4f}"
    )
    return f"{_BASE.rstrip('/')}/map/{bbox}"


def iframe_embed_url(station: dict) -> str:
    """Iframe-friendly URL — same as :func:`portal_deeplink`."""
    return portal_deeplink(station)
