"""Build the AOMC (ASOS Operations and Monitoring Center) catalog.

Source: NCEI Historical Observing Metadata Repository (HOMR)
  https://www.ncei.noaa.gov/access/homr/file/asos-stations.txt

This is the authoritative federal ASOS station list maintained by NWS/FAA/DOD.
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


def _canonical_id(call: str, country: str) -> str | None:
    """Convert a 3-letter CALL to canonical ICAO (K prefix for US, P* for AK/HI/Pacific)."""
    if not call or len(call) != 3:
        return call or None
    # Pacific / Alaska / Hawaii uses 4-char P-prefix codes already (PANC, PHNL).
    # But HOMR stores only the 3-letter CALL, so we need to map by country/state.
    country_u = (country or "").upper()
    if "UNITED STATES" in country_u or country_u == "":
        # CONUS → prefix K
        return "K" + call
    # Guam, Northern Mariana, etc.
    if "GUAM" in country_u:
        return "PG" + call[-2:] if len(call) == 3 else call  # imperfect
    if "MARIANA" in country_u:
        return "PG" + call[-2:] if len(call) == 3 else call
    if "AMERICAN SAMOA" in country_u:
        return "NSTU" if call == "TUT" else "P" + call
    if "PUERTO RICO" in country_u or "VIRGIN ISLANDS" in country_u:
        return "T" + call  # TJSJ, TIST etc.
    if "JAPAN" in country_u or "KOREA" in country_u:
        # Overseas military; leave call as-is but flag differently
        return call
    # Default: prefix K (most US territories)
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

        icao = _canonical_id(call, country)
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
