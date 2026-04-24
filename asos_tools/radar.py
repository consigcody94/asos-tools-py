"""NOAA NEXRAD composite radar tile sources for the 3D globe.

Two complementary free sources, both zero-auth, both NOAA / IEM:

**IEM n0q CONUS composite** (preferred)
    Transparent PNG, known world bounds ``-126,24 → -66,50`` (CONUS), 5-min
    cadence. Perfect for an overlay because the alpha channel lets the
    ASOS globe points shine through. Archived back to 2010 so we can also
    use this for playback mode.

**NOAA RIDGE standard** (fallback)
    Animated GIF, no alpha channel, bigger file size. Useful if IEM is
    slow. The CONUS loop is ``radar.weather.gov/ridge/standard/CONUS_0.gif``.

For Alaska / Hawaii / Puerto Rico we fall back to per-WFO sites since
the IEM CONUS mosaic doesn't cover them.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "latest_composite_url",
    "latest_conus_radar_url",
    "latest_goes_conus_url",
    "latest_goes_fulldisk_url",
    "goes_conus_loop_url",
    "goes_sector_loop_url",
    "goes_loop_for_station",
    "goes18_conus_loop_url",
    "goes18_sector_loop_url",
    "goes18_latest_sector_url",
    "wsr88d_sites",
    "nearest_wsr88d",
    "ridge_loop_url",
    "ridge_still_url",
    "station_radar_loop_url",
    "RADAR_BOUNDS",
]

#: Geographic bounds of the IEM CONUS n0q radar tile (lon_min, lat_min, lon_max, lat_max).
RADAR_BOUNDS = (-126.0, 24.0, -66.0, 50.0)


def _round_to_5min(dt: datetime) -> datetime:
    """IEM posts n0q PNGs at :00/:05/:10/... — round DOWN to the nearest 5-min
    boundary.  Observations land ~90 s after the wall-clock minute, so we also
    back off one full 5-min bucket to dodge 404s from publishing lag.
    """
    dt = dt.replace(second=0, microsecond=0)
    dt = dt.replace(minute=(dt.minute // 5) * 5)
    return dt - timedelta(minutes=5)


def latest_conus_radar_url(now: Optional[datetime] = None) -> str:
    """Return URL of the most recent IEM n0q CONUS composite PNG.

    PNG is transparent, georeferenced to ``RADAR_BOUNDS``. 5-min cadence.
    Drop it onto a Globe.gl tile layer or paste into Folium's ImageOverlay
    with the bounds above.
    """
    ts = _round_to_5min(now or datetime.now(timezone.utc))
    return (
        f"https://mesonet.agron.iastate.edu/archive/data/"
        f"{ts:%Y/%m/%d}/GIS/uscomp/n0q_{ts:%Y%m%d%H%M}.png"
    )


def latest_composite_url(now: Optional[datetime] = None) -> str:
    """Short alias preserved for back-compat / external callers."""
    return latest_conus_radar_url(now)


# --- GOES-19 satellite convenience URLs -------------------------------------
# NOAA NESDIS publishes "latest" imagery with no timestamp so zero date-math
# is needed.  One HTTP request each, cache at the caller for ~5 min.

def latest_goes_conus_url(band: str = "GEOCOLOR") -> str:
    """Latest NESDIS GOES-19 CONUS image.  `band` is the NESDIS folder name
    (GEOCOLOR, AirMass, DayCloudPhase, 13 for clean IR, etc.)."""
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES19/ABI/CONUS/{band}/latest.jpg"
    )


def latest_goes_fulldisk_url(band: str = "GEOCOLOR") -> str:
    """Latest GOES-19 full-disk image — useful when we zoom the globe out to
    see hemisphere-wide cloud cover."""
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES19/ABI/FD/{band}/latest.jpg"
    )


# --- GOES-19 ANIMATED LOOPS (NESDIS pre-rendered GIFs) ----------------------
# NESDIS publishes animated GIF loops of the last N GOES-19 ABI frames at
# stable URLs.  Cadence is 5-min for CONUS sectors, 1-min for MESO-1/2
# sectors.  These are reliable, NESDIS-published, and zero-auth — perfect for a
# "live aviation weather footage" panel in the station drill view.

#: Known NESDIS sector codes for GOES-19 ABI (east-positioned sat).
#: Each sector has a latest JPG + an animated GIF loop.
GOES19_SECTORS = {
    "CONUS": "continental US",
    "FD":    "full disk (western hemisphere)",
    "ne":    "northeast US",
    "umv":   "upper Mississippi valley",
    "sp":    "south Pacific",
    "nr":    "northern Rockies",
    "sr":    "southern Rockies",
    "pr":    "Puerto Rico",
    "can":   "Caribbean",
    "gm":    "Gulf of Mexico",
    "taw":   "tropical Atlantic wide",
    "eep":   "eastern equatorial Pacific",
    "car":   "Caribbean regional",
    "se":    "southeast US",
    "smv":   "southern Mississippi valley",
}


def goes_conus_loop_url(band: str = "GEOCOLOR", size: str = "625x375") -> str:
    """Return URL of NESDIS's animated GIF loop for GOES-19 CONUS.

    Only ``625x375`` is published as a GIF animation for CONUS; larger
    sizes (1250x750, 2500x1500, 5000x3000) exist only as single JPG
    frames.  Keep the default at the published size to avoid 404s.

    Loops ~8–12 of the most recent 5-min frames and refreshes every
    5 minutes.  Image CSP already includes ``*.nesdis.noaa.gov``.
    """
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES19/ABI/CONUS/{band}/"
        f"GOES19-CONUS-{band}-{size}.gif"
    )


def goes_sector_loop_url(
    sector: str, band: str = "GEOCOLOR", size: str = "600x600",
) -> str:
    """Return URL of NESDIS's animated GIF loop for a GOES-19 regional sector.

    Sector must be one of ``GOES19_SECTORS``.  Regional sectors are
    useful when the station's weather is dominated by a local feature
    (e.g. a gulf coast low) and CONUS is too zoomed out.
    """
    sec_upper = sector.upper()
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES19/ABI/SECTOR/{sector}/"
        f"{band}/GOES19-{sec_upper}-{band}-{size}.gif"
    )


# --- GOES-18 West (replaces GOES-17) — covers Pacific, AK, HI, West CONUS ---
# NESDIS publishes the same latest-jpg + animated-gif pattern as GOES-19.
# Sector set differs: ``ak`` (1000x1000), ``hi``/``pnw``/``psw`` (600x600),
# ``wus`` isn't published as a GIF, CONUS same as east at 625x375.

def goes18_conus_loop_url(band: str = "GEOCOLOR", size: str = "625x375") -> str:
    """Animated GIF for GOES-18 CONUS — same coverage as GOES-19 CONUS but
    with a west-biased viewing angle that reads western US storms better."""
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES18/ABI/CONUS/{band}/"
        f"GOES18-CONUS-{band}-{size}.gif"
    )


def goes18_sector_loop_url(
    sector: str, band: str = "GEOCOLOR", size: str = "600x600",
) -> str:
    """Animated GIF for a GOES-18 regional sector.

    Alaska (``ak``) ships at 1000x1000; HI / PNW / PSW at 600x600.
    """
    sec_upper = sector.upper()
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES18/ABI/SECTOR/{sector}/"
        f"{band}/GOES18-{sec_upper}-{band}-{size}.gif"
    )


def goes18_latest_sector_url(sector: str, band: str = "GEOCOLOR") -> str:
    """Latest single-frame JPG for a GOES-18 sector."""
    return (
        f"https://cdn.star.nesdis.noaa.gov/GOES18/ABI/SECTOR/{sector}/"
        f"{band}/latest.jpg"
    )


def goes_loop_for_station(
    lat: float,
    lon: float,
    band: str = "GEOCOLOR",
    size: str = "625x375",
) -> str:
    """Pick the best GOES animated loop URL for a given station lat/lon.

    Routing picks **GOES-19 East** for CONUS/PR/Caribbean and **GOES-18
    West** for Alaska / Hawaii / Pacific NW / Pacific SW. GOES-19 sector
    coverage is tighter in the east; GOES-18 fills the gap west of the
    Rockies and over the Pacific.

      - Puerto Rico / USVI / Caribbean → GOES-19 ``pr`` sector
      - Alaska                         → GOES-18 ``ak`` sector (1000x1000)
      - Hawaii                         → GOES-18 ``hi`` sector (600x600)
      - Pacific Northwest              → GOES-18 ``pnw`` sector (600x600)
      - Pacific Southwest              → GOES-18 ``psw`` sector (600x600)
      - Northeast / Southeast / MS Valley / Rockies → GOES-19 regional
      - Lower-48 fallback              → GOES-19 CONUS loop (625x375)
      - Far-offshore / outside CONUS   → GOES-19 CONUS (no FD GIF exists)
    """
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return goes_conus_loop_url(band=band, size=size)

    # Puerto Rico / USVI / Caribbean
    if 16.0 <= lat_f <= 20.0 and -68.0 <= lon_f <= -63.0:
        return goes_sector_loop_url("pr", band=band, size="600x600")
    # Hawaii — GOES-18 dedicated HI sector
    if 18.0 <= lat_f <= 23.0 and -162.0 <= lon_f <= -154.0:
        return goes18_sector_loop_url("hi", band=band, size="600x600")
    # Alaska — GOES-18 dedicated AK sector (1000x1000 is the published size)
    if lat_f >= 50.0 and lon_f <= -130.0:
        return goes18_sector_loop_url("ak", band=band, size="1000x1000")
    # Pacific Northwest — GOES-18 PNW covers WA/OR/ID/MT west
    if 40.0 <= lat_f <= 50.0 and -130.0 <= lon_f <= -116.0:
        return goes18_sector_loop_url("pnw", band=band, size="600x600")
    # Pacific Southwest — GOES-18 PSW covers CA/NV/AZ/UT west
    if 30.0 <= lat_f <= 40.0 and -125.0 <= lon_f <= -114.0:
        return goes18_sector_loop_url("psw", band=band, size="600x600")
    # Regional CONUS sub-sectors — tighter zoom than full CONUS
    # Northeast US
    if 36.0 <= lat_f <= 48.0 and -85.0 <= lon_f <= -65.0:
        return goes_sector_loop_url("ne", band=band, size="600x600")
    # Southeast US
    if 24.0 <= lat_f <= 37.0 and -92.0 <= lon_f <= -75.0:
        return goes_sector_loop_url("se", band=band, size="600x600")
    # Upper Mississippi Valley
    if 38.0 <= lat_f <= 50.0 and -100.0 <= lon_f <= -85.0:
        return goes_sector_loop_url("umv", band=band, size="600x600")
    # Southern Mississippi Valley
    if 28.0 <= lat_f <= 38.0 and -100.0 <= lon_f <= -85.0:
        return goes_sector_loop_url("smv", band=band, size="600x600")
    # Northern Rockies
    if 40.0 <= lat_f <= 50.0 and -120.0 <= lon_f <= -100.0:
        return goes_sector_loop_url("nr", band=band, size="600x600")
    # Southern Rockies
    if 30.0 <= lat_f <= 40.0 and -120.0 <= lon_f <= -100.0:
        return goes_sector_loop_url("sr", band=band, size="600x600")
    # Lower-48 CONUS (fallback for anything else inside CONUS box)
    if 24.0 <= lat_f <= 50.0 and -126.0 <= lon_f <= -66.0:
        return goes_conus_loop_url(band=band, size=size)
    # Everything outside — NESDIS doesn't publish FD loops, so use CONUS
    return goes_conus_loop_url(band=band, size=size)


# --- NEXRAD WSR-88D RIDGE per-site radar loops ------------------------------
# NWS publishes per-radar animated GIF loops at radar.weather.gov — about
# 10 most-recent base reflectivity frames, refreshed every 5 minutes. These
# are zero-auth, no key, CSP-allowed via *.noaa.gov and give the drill panel
# a station-scoped radar view (vs. the CONUS composite on the globe).

_WSR88D_DATA_PATH = Path(__file__).parent / "data" / "wsr88d_sites.json"


@lru_cache(maxsize=1)
def wsr88d_sites() -> dict[str, dict]:
    """Return the bundled WSR-88D site catalog — ``{id: {name, lat, lon}}``.

    Sourced from ``api.weather.gov/radar/stations?stationType=WSR-88D`` and
    baked into the repo so runtime is offline-safe. 159 sites across CONUS,
    Alaska, Hawaii, Puerto Rico, Guam, and CONUS overseas.
    """
    try:
        with _WSR88D_DATA_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("wsr88d_sites catalog load failed")
        return {}


def nearest_wsr88d(lat: float, lon: float) -> Optional[str]:
    """Return the ICAO id of the WSR-88D site nearest to ``(lat, lon)``.

    Uses spherical-law-of-cosines great-circle distance (plenty accurate at
    WSR-88D spacing, ~230 km between neighbors in CONUS). Returns ``None``
    if no catalog is available or coords are garbage.
    """
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return None
    sites = wsr88d_sites()
    if not sites:
        return None
    lat_r = math.radians(lat_f)
    lon_r = math.radians(lon_f)
    best_id = None
    best_d = float("inf")
    for sid, meta in sites.items():
        try:
            slat = math.radians(float(meta["lat"]))
            slon = math.radians(float(meta["lon"]))
        except (KeyError, TypeError, ValueError):
            continue
        # Great-circle distance (unit sphere — we only need relative order).
        cos_d = (
            math.sin(lat_r) * math.sin(slat)
            + math.cos(lat_r) * math.cos(slat) * math.cos(lon_r - slon)
        )
        cos_d = max(-1.0, min(1.0, cos_d))
        d = math.acos(cos_d)
        if d < best_d:
            best_d = d
            best_id = sid
    return best_id


def ridge_loop_url(site_id: str) -> str:
    """NWS RIDGE animated loop GIF for a WSR-88D site (base reflectivity).

    ``https://radar.weather.gov/ridge/standard/{SITE}_loop.gif`` — ~10 frames,
    5-min cadence, public, CSP-allowed.
    """
    return f"https://radar.weather.gov/ridge/standard/{site_id.upper()}_loop.gif"


def ridge_still_url(site_id: str) -> str:
    """NWS RIDGE latest single-frame GIF for a WSR-88D site."""
    return f"https://radar.weather.gov/ridge/standard/{site_id.upper()}_0.gif"


def station_radar_loop_url(
    lat: Optional[float],
    lon: Optional[float],
    *,
    max_km: float = 400.0,
) -> Optional[str]:
    """Pick the best NEXRAD loop URL for a station.

    Returns the RIDGE per-site loop for the nearest WSR-88D within
    ``max_km``; falls back to the CONUS composite loop when coords fall
    outside any radar's effective coverage (or when catalog load fails).
    Returns ``None`` if coordinates cannot be parsed at all.
    """
    try:
        lat_f = float(lat)  # type: ignore[arg-type]
        lon_f = float(lon)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    site_id = nearest_wsr88d(lat_f, lon_f)
    if not site_id:
        return "https://radar.weather.gov/ridge/standard/CONUS_0.gif"
    # Sanity-check distance to avoid pointing an Alaska station at a
    # CONUS radar. 1 degree ≈ 111 km.
    meta = wsr88d_sites().get(site_id, {})
    try:
        slat = float(meta["lat"])
        slon = float(meta["lon"])
        dlat = (lat_f - slat) * 111.0
        dlon = (lon_f - slon) * 111.0 * math.cos(math.radians((lat_f + slat) / 2))
        km = math.hypot(dlat, dlon)
    except (KeyError, TypeError, ValueError):
        km = 0.0
    if km > max_km:
        return "https://radar.weather.gov/ridge/standard/CONUS_0.gif"
    return ridge_loop_url(site_id)


def head_ok(url: str, timeout: float = 5.0) -> bool:
    """Cheap reachability check — used when the UI wants to gray out a layer
    toggle if the upstream is 5xxing."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          headers={"User-Agent": "owl.observation-watch-log/1.0"})
        return r.status_code == 200
    except Exception:
        return False
