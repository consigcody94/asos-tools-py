"""NOAA National Data Buoy Center (NDBC) — marine met observations.

``ndbc.noaa.gov`` publishes fixed-width text feeds of wind, pressure,
temperature, and wave data from ~400 met-enabled buoys and coastal
CMAN stations. Feeds are public, no auth, and updated every 10-30 min.

This module exposes:

- ``nearest_buoy(lat, lon)``  — pick the nearest met-enabled station from
  the bundled catalog (``data/ndbc_met_stations.json``, snapshot of
  ``activestations.xml``).
- ``fetch_latest(buoy_id)``   — return the newest ``.txt`` observation as a
  dict (parses the standard realtime2 2-line header).
- ``observations_near(lat, lon)`` — one-shot "nearest buoy + its latest
  obs + distance" wrapper used by the drill panel.

Coastal ASOS stations benefit from a wind/pressure cross-check against
the nearest offshore buoy — a legitimate integrity signal for flagging
false maintenance-flags (`$`) or MISSING-classification false positives.
"""

from __future__ import annotations

import json
import logging
import math
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "buoy_catalog",
    "nearest_buoy",
    "fetch_latest",
    "observations_near",
    "NDBC_BASE",
]

NDBC_BASE = "https://www.ndbc.noaa.gov"

_HEADERS = {
    "User-Agent": "owl.observation-watch-log/1.0 (github.com/consigcody94/asos-tools-py)",
    "Accept": "text/plain, */*",
}

_CATALOG_PATH = Path(__file__).parent / "data" / "ndbc_met_stations.json"


@lru_cache(maxsize=1)
def buoy_catalog() -> dict[str, dict]:
    """``{id: {name, lat, lon, type, owner}}`` for every met-enabled NDBC
    station. ~402 entries, bundled at 48 KB."""
    try:
        with _CATALOG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("ndbc catalog load failed")
        return {}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def nearest_buoy(
    lat: float,
    lon: float,
    *,
    max_km: float = 200.0,
) -> Optional[dict]:
    """Return ``{id, name, lat, lon, distance_km, ...}`` for the nearest
    met-enabled NDBC station within ``max_km``, else ``None``.

    Default radius is 200 km — beyond that the surface wind/pressure
    comparison is no longer meaningful for coastal ASOS sanity checking.
    """
    try:
        qlat = float(lat)
        qlon = float(lon)
    except (TypeError, ValueError):
        return None
    cat = buoy_catalog()
    if not cat:
        return None
    best = None
    best_d = float("inf")
    for bid, meta in cat.items():
        try:
            blat = float(meta["lat"])
            blon = float(meta["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        d = _haversine_km(qlat, qlon, blat, blon)
        if d < best_d:
            best_d = d
            best = {**meta, "id": bid, "distance_km": round(d, 1)}
    if best is None or best_d > max_km:
        return None
    return best


#: Column order of the standard realtime2 ``.txt`` feed.
#: ``#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE``
_REALTIME2_COLS = [
    "yy", "mm", "dd", "hh", "mn",
    "wind_dir_deg", "wind_mps", "gust_mps",
    "wave_ht_m", "dom_period_s", "avg_period_s", "mean_wave_dir_deg",
    "pres_hpa", "air_c", "water_c", "dew_c",
    "vis_nm", "ptdy_hpa", "tide_ft",
]


def _parse_realtime2_first_row(text: str) -> Optional[dict]:
    """Parse the newest observation out of a realtime2 ``.txt`` feed.

    Rows are newest-first. Two leading lines are headers (``#YY`` + ``#yr``);
    after that the first data row is the latest report. ``MM`` is the
    missing-value sentinel — coerced to ``None``.
    """
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < len(_REALTIME2_COLS):
            continue
        row: dict = {}
        for col, val in zip(_REALTIME2_COLS, parts):
            if val == "MM":
                row[col] = None
            else:
                try:
                    row[col] = float(val) if col not in ("yy", "mm", "dd", "hh", "mn") else int(val)
                except ValueError:
                    row[col] = val
        # Convert units to match ASOS conventions where cheap.
        if row.get("wind_mps") is not None:
            row["wind_kt"] = round(row["wind_mps"] * 1.94384, 1)
        if row.get("gust_mps") is not None:
            row["gust_kt"] = round(row["gust_mps"] * 1.94384, 1)
        if row.get("pres_hpa") is not None:
            row["pres_inhg"] = round(row["pres_hpa"] / 33.8639, 2)
        if row.get("air_c") is not None:
            row["air_f"] = round(row["air_c"] * 9 / 5 + 32, 1)
        if row.get("water_c") is not None:
            row["water_f"] = round(row["water_c"] * 9 / 5 + 32, 1)
        if row.get("dew_c") is not None:
            row["dew_f"] = round(row["dew_c"] * 9 / 5 + 32, 1)
        return row
    return None


@lru_cache(maxsize=64)
def _cached_fetch(buoy_id: str, bust: str) -> Optional[tuple]:
    url = f"{NDBC_BASE}/data/realtime2/{buoy_id}.txt"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12.0)
        if r.status_code != 200:
            return None
        row = _parse_realtime2_first_row(r.text)
    except Exception:
        logger.exception("ndbc fetch %s failed", buoy_id)
        return None
    if not row:
        return None
    # Return as sorted tuple of (key, value) for hashability.
    return tuple(sorted(row.items(), key=lambda kv: kv[0]))


def fetch_latest(buoy_id: str) -> Optional[dict]:
    """Latest parsed observation for ``buoy_id``. Returns ``None`` on miss
    or parse failure. Cached ~10 min (NDBC publishes at 10-30 min cadence).
    """
    if not buoy_id:
        return None
    bust = str(int(time.time() // 600))  # 10-min bucket
    row = _cached_fetch(buoy_id.strip().upper(), bust)
    if row is None:
        return None
    return dict(row)


def observations_near(
    lat: float,
    lon: float,
    *,
    max_km: float = 200.0,
) -> Optional[dict]:
    """Find the nearest buoy and return its station meta + latest obs.

    Returns ``{buoy: {...}, obs: {...}}`` or ``None`` when no buoy is
    within range or the feed is unreachable. Drop-in for a drill-panel
    coastal cross-check widget.
    """
    near = nearest_buoy(lat, lon, max_km=max_km)
    if not near:
        return None
    obs = fetch_latest(near["id"])
    if not obs:
        return {"buoy": near, "obs": None}
    return {"buoy": near, "obs": obs}
