"""Structured METAR decoder built on avwx-engine.

We keep the existing ``has_maintenance_flag`` + ``decode_maintenance_reasons``
logic (see :mod:`asos_tools.metars`) because the ``$`` flag and missing-hour
detection are O.W.L.'s core competency.  This module adds a *second* layer
of structured fields on top:

* **flight category** — VFR / MVFR / IFR / LIFR (pre-computed by avwx)
* **numeric visibility / ceiling / wind / temp / dew / altimeter**
* **typed sensor-status indicators** — RVRNO, PWINO, TSNO, etc., as a list
* **wind variable / gust direction ranges**
* **TAF + ATIS** parsing when the raw text is one of those

Usage:

>>> from asos_tools.metar_parse import parse_metar
>>> info = parse_metar(raw_metar_text)
>>> info["flight_category"]  # 'MVFR'
>>> info["sensor_status"]    # ['RVRNO']
>>> info["visibility_sm"]    # 10.0
>>> info["ceiling_ft"]       # 2700
>>> info["has_maintenance"]  # True (we still compute this ourselves)
"""

from __future__ import annotations

import logging
from typing import Any, Optional

try:
    from avwx import Metar   # type: ignore
    _HAVE_AVWX = True
except ImportError:          # pragma: no cover
    _HAVE_AVWX = False

from asos_tools.metars import (
    has_maintenance_flag,
    decode_maintenance_reasons,
    decode_reasons_short,
)

logger = logging.getLogger(__name__)

__all__ = [
    "parse_metar",
    "METAR_SENSOR_FIELDS",
    "sensor_health_grid",
]


# The set of sensor health fields that show up as distinct RMK codes.
# This drives the per-sensor grid in the drill panel.
METAR_SENSOR_FIELDS: list[tuple[str, str, str]] = [
    # (display name, matching remark code, description)
    ("Wind",        "WND",     "10-min mean wind + gust"),
    ("Temp/Dew",    "TMPDEW",  "Temperature & dewpoint sensors"),
    ("Altimeter",   "ALT",     "Pressure altimeter"),
    ("Visibility",  "VIS",     "Prevailing visibility sensor"),
    ("Ceiling",     "CHINO",   "Ceilometer (cloud height)"),
    ("Precip amt",  "PNO",     "Precipitation accumulation gauge"),
    ("Present wx",  "PWINO",   "Present-weather identifier (RA/SN/etc)"),
    ("Lightning",   "TSNO",    "Lightning/thunderstorm detection"),
    ("Freezing rn", "FZRANO",  "Freezing-rain sensor"),
    ("RVR",         "RVRNO",   "Runway visual range"),
]


def _first(obj: Any, *attrs, default: Any = None) -> Any:
    """Return first non-None attribute value from ``obj`` in ``attrs``."""
    for a in attrs:
        v = getattr(obj, a, None)
        if v is not None:
            return v
    return default


def _ceiling_ft(clouds: list[Any]) -> Optional[int]:
    """Return the height (ft) of the lowest BKN/OVC/OVX cloud layer."""
    if not clouds:
        return None
    for c in clouds:
        cov = (getattr(c, "type", "") or "").upper()
        if cov in {"BKN", "OVC", "OVX"}:
            # avwx stores base height as `base` (hundreds of ft) but renders it
            # via `altitude` on some Cloud variants.  Try both.
            base = getattr(c, "base", None) or getattr(c, "altitude", None)
            if base is not None:
                try:
                    return int(base) * 100
                except (TypeError, ValueError):
                    pass
    return None


def _visibility_sm(v: Any) -> Optional[float]:
    """Return prevailing visibility in statute miles (float), None if missing."""
    if v is None:
        return None
    val = getattr(v, "value", None)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _num(n: Any) -> Optional[float]:
    """Pull a numeric value from an avwx Number-ish field."""
    if n is None:
        return None
    val = getattr(n, "value", None)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def parse_metar(raw: str, station: Optional[str] = None) -> dict:
    """Parse a METAR line into a flat dict of structured fields.

    Always returns a dict.  Falls back gracefully if avwx-engine isn't
    installed (still provides ``has_maintenance`` from our own regex).
    """
    out: dict[str, Any] = {
        "raw": raw or "",
        "station": station,
        # Our core fields (computed without avwx).
        "has_maintenance": has_maintenance_flag(raw),
        "maintenance_reasons": decode_maintenance_reasons(raw),
        "maintenance_short": decode_reasons_short(raw),
        # Avwx-provided structured fields (None if parsing fails).
        "flight_category": None,
        "visibility_sm": None,
        "ceiling_ft": None,
        "wind_speed_kt": None,
        "wind_gust_kt": None,
        "wind_direction_deg": None,
        "temperature_c": None,
        "dewpoint_c": None,
        "altimeter_inhg": None,
        "sensor_status": [],     # list of RMK codes present in remarks
        "clouds": [],            # list of {type, base_ft}
        "wx_codes": [],
        "time_utc": None,
    }
    if not raw or not _HAVE_AVWX:
        return out

    try:
        m = Metar(station or "KJFK")
        m.parse(raw)
        d = m.data
    except Exception:
        logger.debug("avwx parse failed for: %s", raw[:80])
        return out

    out["flight_category"] = _first(d, "flight_rules")
    out["visibility_sm"] = _visibility_sm(getattr(d, "visibility", None))
    out["wind_speed_kt"] = _num(getattr(d, "wind_speed", None))
    out["wind_gust_kt"] = _num(getattr(d, "wind_gust", None))
    out["wind_direction_deg"] = _num(getattr(d, "wind_direction", None))
    out["temperature_c"] = _num(getattr(d, "temperature", None))
    out["dewpoint_c"] = _num(getattr(d, "dewpoint", None))
    out["altimeter_inhg"] = _num(getattr(d, "altimeter", None))

    clouds = getattr(d, "clouds", None) or []
    out["ceiling_ft"] = _ceiling_ft(clouds)
    out["clouds"] = [
        {"type": getattr(c, "type", None),
         "base_ft": (int(getattr(c, "base", 0) or 0) * 100) or None}
        for c in clouds
    ]

    wx = getattr(d, "wx_codes", None) or []
    out["wx_codes"] = [getattr(w, "repr", "") or str(w) for w in wx]

    t = getattr(d, "time", None)
    if t and getattr(t, "dt", None):
        out["time_utc"] = t.dt.isoformat(timespec="minutes")

    # Sensor status indicators — avwx surfaces these via remarks_info if
    # they appear in the RMK section.  We also do our own regex against
    # the raw text because avwx's parser occasionally misses odd formats.
    rmk = getattr(d, "remarks_info", None)
    status: list[str] = []
    upper = raw.upper()
    for code in ("RVRNO", "PWINO", "PNO", "FZRANO", "TSNO", "SLPNO",
                 "VISNO", "CHINO"):
        if code in upper:
            status.append(code)
    if rmk:
        # Some avwx versions expose a `sensor_status` list of typed codes.
        sens = (getattr(rmk, "sensor_status_indicators", None)
                or getattr(rmk, "sensor_status", None))
        if isinstance(sens, list):
            for s in sens:
                v = getattr(s, "repr", None) or str(s)
                if v and v not in status:
                    status.append(v)
    out["sensor_status"] = status

    return out


def sensor_health_grid(parsed: dict) -> list[dict]:
    """Return a list of per-sensor health rows for the drill panel grid.

    Each row: ``{sensor, label, ok, reason}``.
    ``ok`` is True (green), False (red = explicitly failed), or None (gray
    = unknown / not covered by this observation).
    """
    if not parsed:
        return []
    sensor_status = set((parsed.get("sensor_status") or []))
    has_maint = bool(parsed.get("has_maintenance"))
    raw_upper = (parsed.get("raw") or "").upper()

    rows: list[dict] = []

    def _row(name: str, ok: Optional[bool], reason: str = "") -> dict:
        return {"sensor": name, "ok": ok, "reason": reason}

    # Wind
    if parsed.get("wind_speed_kt") is not None:
        gust = parsed.get("wind_gust_kt")
        detail = f"{int(parsed['wind_speed_kt'])} kt"
        if gust:
            detail += f" gusting {int(gust)} kt"
        rows.append(_row("Wind", True, detail))
    else:
        missing = "/////KT" in raw_upper
        rows.append(_row("Wind", False if missing else None,
                         "missing wind group" if missing else ""))

    # Temp / Dew
    if parsed.get("temperature_c") is not None:
        t = parsed["temperature_c"]
        d = parsed.get("dewpoint_c")
        rows.append(_row("Temp/Dew", True,
                         f"{t:+.0f}/{d:+.0f} degC" if d is not None
                         else f"{t:+.0f} degC"))
    else:
        rows.append(_row("Temp/Dew",
                         False if "M/M" in raw_upper else None,
                         "M/M in obs" if "M/M" in raw_upper else ""))

    # Altimeter
    if parsed.get("altimeter_inhg") is not None:
        rows.append(_row("Altimeter", True,
                         f"{parsed['altimeter_inhg']:.2f} inHg"))
    else:
        rows.append(_row("Altimeter", False if " A////" in raw_upper else None,
                         "altimeter missing" if " A////" in raw_upper else ""))

    # Visibility
    if "VISNO" in sensor_status:
        rows.append(_row("Visibility", False, "VISNO reported"))
    elif parsed.get("visibility_sm") is not None:
        rows.append(_row("Visibility", True,
                         f"{parsed['visibility_sm']:g} SM"))
    else:
        rows.append(_row("Visibility", None, ""))

    # Ceiling
    if "CHINO" in sensor_status:
        rows.append(_row("Ceiling", False, "CHINO (ceilometer out)"))
    elif parsed.get("ceiling_ft") is not None:
        rows.append(_row("Ceiling", True, f"{parsed['ceiling_ft']:,} ft"))
    else:
        rows.append(_row("Ceiling", None, "clear of BKN/OVC"))

    # Precip
    rows.append(_row("Precip amt",
                     False if "PNO" in sensor_status else None,
                     "PNO reported" if "PNO" in sensor_status else ""))

    # Present weather identifier
    rows.append(_row("Present wx",
                     False if "PWINO" in sensor_status else None,
                     "PWINO reported" if "PWINO" in sensor_status else ""))

    # Lightning / thunderstorm sensor
    rows.append(_row("Lightning",
                     False if "TSNO" in sensor_status else None,
                     "TSNO reported" if "TSNO" in sensor_status else ""))

    # Freezing rain
    rows.append(_row("Freezing rn",
                     False if "FZRANO" in sensor_status else None,
                     "FZRANO reported" if "FZRANO" in sensor_status else ""))

    # RVR
    rows.append(_row("RVR",
                     False if "RVRNO" in sensor_status else None,
                     "RVRNO reported" if "RVRNO" in sensor_status else ""))

    # Pressure (SLP)
    rows.append(_row("Sea-level P",
                     False if "SLPNO" in sensor_status else None,
                     "SLPNO reported" if "SLPNO" in sensor_status else ""))

    # Overall `$` flag indicator at the end.
    if has_maint:
        rows.append(_row("$ indicator", False,
                         "ASOS self-flag ($) present"))

    return rows
