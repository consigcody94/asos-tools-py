"""NOAA SWPC space-weather client.

All endpoints here are zero-auth, NOAA SWPC authoritative, and typically
return JSON arrays.  Space weather matters to aviation because:

* **Kp ≥ 5** degrades HF (high-frequency) radio comms used over oceanic
  ATC and by field maintenance techs operating in remote areas.
* **Solar flare X-ray class M / X** causes short-wave radio blackouts
  (Space Weather Prediction Center "R" scale).
* **Geomagnetic storms** can disrupt GPS integrity used for GNSS
  approaches, and in extreme cases affect transformer/power grids
  supporting remote ASOS installations.

This module surfaces three live datasets:

* Planetary Kp index (3-hour update)
* GOES X-ray flux (1-minute update)
* Active SWPC alerts (event-driven)

All functions return plain dicts/lists, never raise — upstream outages
just produce empty results.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

__all__ = [
    "current_kp",
    "current_xray",
    "current_alerts",
    "space_weather_summary",
    "KP_SCALE_NAMES",
]

_BASE = "https://services.swpc.noaa.gov"
_UA = "owl.observation-watch-log/1.0"


#: NOAA G-scale: Kp index -> storm level label
KP_SCALE_NAMES = {
    0: "Quiet", 1: "Quiet", 2: "Quiet", 3: "Unsettled", 4: "Active",
    5: "G1 Minor", 6: "G2 Moderate", 7: "G3 Strong", 8: "G4 Severe",
    9: "G5 Extreme",
}


# ---- tiny in-process cache so rerenders don't hammer SWPC --------------
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_TTL_S = 180


def _get_json(path: str) -> Optional[object]:
    now = time.time()
    hit = _CACHE.get(path)
    if hit and now - hit[0] < _CACHE_TTL_S:
        return hit[1]
    try:
        r = requests.get(
            f"{_BASE}{path}",
            headers={"User-Agent": _UA},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        _CACHE[path] = (now, data)
        return data
    except Exception:
        logger.exception("SWPC %s failed", path)
        return None


# ---- Kp index --------------------------------------------------------------
def current_kp() -> dict:
    """Return the most recent Kp reading + human label + storm tier.

    Schema::

        {
          "kp": 3,
          "time_tag_utc": "2026-04-19T21:00:00Z",
          "label": "Unsettled",
          "storm_level": 0,           # 0 = below G1, 5..9 = G1..G5
          "source": "SWPC planetary K-index",
        }
    """
    data = _get_json("/products/noaa-planetary-k-index.json")
    out = {
        "kp": None, "time_tag_utc": None, "label": "unknown",
        "storm_level": None, "kp_float": None,
        "source": "NOAA SWPC planetary K-index",
    }
    if not isinstance(data, list) or not data:
        return out
    # SWPC publishes a list of dict rows like
    #   {"time_tag": "2026-04-20T18:00:00", "Kp": 5.0, "a_running": 48, ...}
    # (earlier versions of the feed used a list-of-lists + header row, so
    # we handle both shapes defensively.)
    latest: dict = {}
    last = data[-1]
    if isinstance(last, dict):
        latest = last
    elif isinstance(last, list) and isinstance(data[0], list):
        header = data[0]
        try:
            latest = dict(zip(header, last))
        except Exception:
            return out
    if not latest:
        return out
    try:
        kp_float = float(latest.get("Kp") or latest.get("kp") or 0)
    except (TypeError, ValueError):
        return out
    kp_int = int(round(kp_float))
    out.update({
        "kp": kp_int,
        "kp_float": round(kp_float, 2),
        "time_tag_utc": latest.get("time_tag"),
        "label": KP_SCALE_NAMES.get(kp_int, "Unknown"),
        "storm_level": (kp_int - 4) if kp_int >= 5 else 0,
    })
    return out


# ---- X-ray flux (solar flares) --------------------------------------------
def current_xray() -> dict:
    """Return the most recent GOES long-channel X-ray flux + flare class."""
    data = _get_json("/json/goes/primary/xrays-1-day.json")
    out = {
        "flux_wm2": None, "class": None, "time_tag_utc": None,
        "source": "NOAA SWPC GOES primary X-ray",
    }
    if not isinstance(data, list) or not data:
        return out
    # Prefer the 0.1–0.8 nm long channel — that's the HF blackout proxy.
    longs = [d for d in data if d.get("energy") == "0.1-0.8nm"]
    latest = longs[-1] if longs else data[-1]
    try:
        flux = float(latest.get("flux"))
    except (TypeError, ValueError):
        return out
    # Flare class mapping: A < 1e-8, B < 1e-7, C < 1e-6, M < 1e-5, X >= 1e-5.
    if flux < 1e-8:
        cls = "A"
    elif flux < 1e-7:
        cls = "B"
    elif flux < 1e-6:
        cls = "C"
    elif flux < 1e-5:
        cls = "M"
    else:
        cls = "X"
    # Append decimal multiplier (e.g. "C2.3" = 2.3e-6 W/m2).
    try:
        base = {"A":1e-9, "B":1e-8, "C":1e-7, "M":1e-6, "X":1e-5}[cls]
        mult = flux / base
        cls_str = f"{cls}{mult:.1f}"
    except Exception:
        cls_str = cls
    out.update({
        "flux_wm2": flux,
        "class": cls_str,
        "time_tag_utc": latest.get("time_tag"),
    })
    return out


# ---- Active SWPC alerts ----------------------------------------------------
def current_alerts(limit: int = 5) -> list[dict]:
    """Return up to ``limit`` most recent SWPC watches / warnings / alerts."""
    data = _get_json("/products/alerts.json")
    if not isinstance(data, list):
        return []
    # Each entry has {product_id, issue_datetime, message, ...}
    out = []
    for row in data[-limit:][::-1]:
        out.append({
            "id":       row.get("product_id") or row.get("id"),
            "time_utc": row.get("issue_datetime") or row.get("time_tag"),
            "message":  (row.get("message") or "")[:400],
        })
    return out


def space_weather_summary() -> dict:
    """One-call rollup used by the NWS Forecasters tab."""
    return {
        "kp":     current_kp(),
        "xray":   current_xray(),
        "alerts": current_alerts(limit=3),
    }
