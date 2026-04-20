"""NOAA NEXRAD composite radar tile sources for the 3D globe.

Two complementary free sources, both zero-auth, both federal:

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

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "latest_composite_url",
    "latest_conus_radar_url",
    "latest_goes_conus_url",
    "latest_goes_fulldisk_url",
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


def head_ok(url: str, timeout: float = 5.0) -> bool:
    """Cheap reachability check — used when the UI wants to gray out a layer
    toggle if the upstream is 5xxing."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          headers={"User-Agent": "owl.observation-watch-log/1.0"})
        return r.status_code == 200
    except Exception:
        return False
