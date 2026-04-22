"""Build the AOMC (ASOS Operations and Monitoring Center) catalog.

Source: NCEI Historical Observing Metadata Repository (HOMR)
  https://www.ncei.noaa.gov/access/homr/file/asos-stations.txt

This is the authoritative ASOS station list maintained by NWS/FAA/DOD.
It contains the roughly 900 "true" ASOS sites (commissioned, certified, and
operated by the government) -- as opposed to the IEM catalog which also
includes AWOS-only sites.

The file is a fixed-width text table. We parse it to JSON and bundle it
under asos_tools/data/aomc_stations.json so the package ships with the
official list and doesn't need a network call at runtime.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://www.ncei.noaa.gov/access/homr/file/asos-stations.txt"


def _maybe(value: str) -> str | None:
    """Normalize empty-like placeholders to None."""
    if value is None:
        return None
    v = value.strip()
    if not v or v in {"-99999", "UNKNOWN"}:
        return None
    return v


def _float_or_none(value: str) -> float | None:
    v = _maybe(value)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _int_or_none(value: str) -> int | None:
    v = _maybe(value)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _parse(text: str) -> list[dict]:
    lines = text.splitlines()
    if len(lines) < 3:
        raise ValueError("asos-stations.txt has fewer than 3 lines")

    header = lines[0]
    divider = lines[1]

    # The divider line is blocks of dashes separated by whitespace; use it to
    # find the column offsets.
    col_spans: list[tuple[int, int]] = []
    for m in re.finditer(r"-+", divider):
        col_spans.append((m.start(), m.end()))

    # Extract column names from the header using those same spans.
    col_names: list[str] = []
    for start, end in col_spans:
        name = header[start:end].strip().upper()
        col_names.append(name)

    rows: list[dict] = []
    for raw in lines[2:]:
        if not raw.strip():
            continue
        record: dict[str, str] = {}
        for name, (start, end) in zip(col_names, col_spans):
            record[name] = raw[start:end].strip()
        # The last column may extend past the last divider span; grab tail.
        if col_spans:
            tail_start = col_spans[-1][1]
            if len(raw) > tail_start:
                tail = raw[tail_start:].strip()
                if tail:
                    record[col_names[-1]] = (record[col_names[-1]] + " " + tail).strip()
        rows.append(record)
    return rows


#: Lazy-loaded FAA-LID -> canonical-ICAO map, populated from AWC
#: (``aviationweather.gov/api/data/stationinfo``) on first call to
#: :func:`_canonical_id`.  Covers Alaska (PA*), Hawaii (PH*), Puerto Rico
#: (TJ*), US Virgin Islands (TI*), Guam (PG*), American Samoa (NS*),
#: and the Northern Mariana Islands.  The previous version of this
#: function did ``"K" + call`` for everything, which is wrong for any
#: non-CONUS station and produced bogus IDs like KANC (actual: PANC).
_FAA_TO_ICAO_CACHE: dict[str, str] | None = None


def _load_awc_faa_map() -> dict[str, str]:
    """Fetch and cache the AWC FAA->ICAO mapping for non-CONUS stations."""
    global _FAA_TO_ICAO_CACHE
    if _FAA_TO_ICAO_CACHE is not None:
        return _FAA_TO_ICAO_CACHE
    import json as _json
    mapping: dict[str, str] = {}
    # Three bbox queries cover every US non-CONUS region that has ASOS.
    for bbox in (
        "51,-180,72,-130",    # Alaska + Aleutians
        "14,-180,24,-154",    # Hawaii + Pacific islands
        "17,-68,19,-64",      # Puerto Rico + US Virgin Islands
        "13,144,16,146",      # Guam / Northern Marianas
    ):
        try:
            r = requests.get(
                "https://aviationweather.gov/api/data/stationinfo",
                params={"bbox": bbox, "format": "json"},
                headers={"User-Agent": "owl.observation-watch-log/1.0"},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            for s in r.json() or []:
                faa = (s.get("faaId") or "").strip().upper()
                icao = (s.get("icaoId") or "").strip().upper()
                # Only keep non-CONUS ICAOs (starts with P, T, N).
                if faa and icao and icao[:1] in "PTN":
                    mapping[faa] = icao
        except Exception:
            continue
    _FAA_TO_ICAO_CACHE = mapping
    return mapping


def _canonical_id(call: str, country: str, state: str = "") -> str | None:
    """Convert a 3-letter FAA LID to canonical ICAO.

    CONUS uses a simple ``"K" + call`` rule (KJFK, KLGA).  Non-CONUS
    (Alaska, Hawaii, Guam, Puerto Rico, USVI, American Samoa, Marianas)
    uses irregular mappings — e.g. FAA ``FAI`` -> ICAO ``PAFA``,
    ``KOA`` -> ``PHKO``, ``SJU`` -> ``TJSJ``.  We resolve those via the
    Aviation Weather Center's authoritative stationinfo endpoint.
    """
    if not call:
        return None
    call = call.strip().upper()
    state = (state or "").strip().upper()

    NON_CONUS_STATES = {"AK", "HI", "GU", "MP", "AS", "PR", "VI"}

    # For non-CONUS US stations, always prefer AWC's authoritative mapping.
    if state in NON_CONUS_STATES:
        m = _load_awc_faa_map()
        icao = m.get(call)
        if icao:
            return icao
        # Fallback: best-effort prefix if AWC couldn't resolve (rare —
        # usually means the station is decommissioned).
        _FALLBACK_PREFIX = {
            "AK": "PA", "HI": "PH", "GU": "PG", "MP": "PG",
            "AS": "NS", "PR": "TJ", "VI": "TI",
        }
        pref = _FALLBACK_PREFIX.get(state, "")
        if pref and len(call) == 3:
            return pref + call        # approximate; may not match AWC
        return call

    # 3-character military-apron codes that aren't really airports
    # (e.g. "CO90") should be left as-is.
    if len(call) != 3:
        return call

    # CONUS + territories not covered above: K-prefix.
    return "K" + call


def main() -> int:
    print(f"Fetching {URL}")
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    text = r.text

    records = _parse(text)
    print(f"Parsed {len(records):,} raw records")

    stations: list[dict] = []
    for rec in records:
        call = rec.get("CALL", "").strip()
        name = rec.get("NAME", "").strip()
        country = rec.get("COUNTRY", "").strip()
        state = rec.get("ST", "").strip()

        # Skip rows missing a call sign.
        if not call:
            continue

        icao = _canonical_id(call, country, state=rec.get("STATE", ""))
        stations.append({
            "id": icao,
            "call": call,
            "wban": _maybe(rec.get("WBAN", "")),
            "coop_id": _maybe(rec.get("COOPID", "")),
            "ncdc_id": _maybe(rec.get("NCDCID", "")),
            "ghcnd_id": _maybe(rec.get("GHCND", "")),
            "name": name,
            "alt_name": _maybe(rec.get("ALT_NAME", "")),
            "country": country,
            "state": state,
            "county": _maybe(rec.get("COUNTY", "")),
            "lat": _float_or_none(rec.get("LAT", "")),
            "lon": _float_or_none(rec.get("LON", "")),
            "elev_ft": _int_or_none(rec.get("ELEV", "")),
            "utc_offset_hr": _int_or_none(rec.get("UTC", "")),
            "station_types": _maybe(rec.get("STNTYPE", "")),
            "begin_date": _maybe(rec.get("BEGDT", "")),
            "elev_p": _int_or_none(rec.get("ELEV_P", "")),
            "elev_a": _int_or_none(rec.get("ELEV_A", "")),
        })

    # Dedup by canonical id (HOMR sometimes has duplicates).
    seen: dict[str, dict] = {}
    for s in stations:
        if s["id"] and s["id"] not in seen:
            seen[s["id"]] = s
    stations = sorted(seen.values(), key=lambda r: r["id"])

    output = {
        "source": URL,
        "fetched_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "record_count": len(stations),
        "stations": stations,
    }

    out = Path(__file__).resolve().parent.parent / "asos_tools" / "data" / "aomc_stations.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, indent=2))
    print(f"Wrote {len(stations):,} AOMC stations to {out}")

    # Quick sanity sample
    print("\nSample:")
    for s in stations[:3]:
        print(f"  {s['id']:6}  {s['name']:<32}  {s['state']}  {s['country']}")
    print("  …")
    for s in stations[-2:]:
        print(f"  {s['id']:6}  {s['name']:<32}  {s['state']}  {s['country']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
