"""Build the bundled ASOS station catalog from IEM.

Pulls the per-state/territory ASOS network GeoJSON feeds, normalizes the
records into a compact JSON array, and writes it to
``asos_tools/data/stations.json``. Commit the output so runtime lookups
don't require a network call.

Run from the repo root::

    python deploy/build_station_catalog.py
"""

from __future__ import annotations

import json
from pathlib import Path

import requests


# 50 states + DC + territories + Alaska + Hawaii
US_STATE_NETWORKS = [
    "AL_ASOS", "AK_ASOS", "AZ_ASOS", "AR_ASOS", "CA_ASOS", "CO_ASOS",
    "CT_ASOS", "DE_ASOS", "DC_ASOS", "FL_ASOS", "GA_ASOS", "HI_ASOS",
    "ID_ASOS", "IL_ASOS", "IN_ASOS", "IA_ASOS", "KS_ASOS", "KY_ASOS",
    "LA_ASOS", "ME_ASOS", "MD_ASOS", "MA_ASOS", "MI_ASOS", "MN_ASOS",
    "MS_ASOS", "MO_ASOS", "MT_ASOS", "NE_ASOS", "NV_ASOS", "NH_ASOS",
    "NJ_ASOS", "NM_ASOS", "NY_ASOS", "NC_ASOS", "ND_ASOS", "OH_ASOS",
    "OK_ASOS", "OR_ASOS", "PA_ASOS", "RI_ASOS", "SC_ASOS", "SD_ASOS",
    "TN_ASOS", "TX_ASOS", "UT_ASOS", "VT_ASOS", "VA_ASOS", "WA_ASOS",
    "WV_ASOS", "WI_ASOS", "WY_ASOS",
    # Territories
    "PR_ASOS", "VI_ASOS", "GU_ASOS", "AS_ASOS", "MP_ASOS",
]


def _fetch_network(net: str, session: requests.Session) -> list[dict]:
    url = f"https://mesonet.agron.iastate.edu/geojson/network/{net}.geojson"
    r = session.get(url, timeout=60)
    if not r.ok:
        return []
    feats = r.json().get("features", [])
    return [
        {
            "sid": f["properties"].get("sid"),
            "name": (f["properties"].get("sname") or "").strip(),
            "state": f["properties"].get("state"),
            "country": f["properties"].get("country"),
            "lon": f["geometry"]["coordinates"][0],
            "lat": f["geometry"]["coordinates"][1],
            "elevation": f["properties"].get("elevation"),
            "network": net,
            "online": bool(f["properties"].get("online")),
            "archive_begin": f["properties"].get("archive_begin"),
            "archive_end": f["properties"].get("archive_end"),
        }
        for f in feats
        if f.get("properties", {}).get("sid")
    ]


def _canonical_icao(sid: str, network: str) -> str:
    """Return the ICAO identifier most users type (e.g. 'KALB', 'PANC')."""
    if not sid:
        return sid
    # Alaska/Hawaii/Pacific already use full 4-char ICAO (PANC, PHNL, PGUM…).
    if len(sid) == 4 and sid[0] in ("P",):
        return sid
    # CONUS / PR / VI / DC etc. use 3-letter FAA → prefix 'K' unless already.
    if len(sid) == 3:
        return "K" + sid
    return sid


def main() -> int:
    session = requests.Session()
    all_rows: list[dict] = []
    seen_ids: set[str] = set()

    for net in US_STATE_NETWORKS:
        print(f"Fetching {net} …", end="", flush=True)
        stations = _fetch_network(net, session)
        print(f" {len(stations)} stations")
        for s in stations:
            icao = _canonical_icao(s["sid"], s["network"])
            if icao in seen_ids:
                continue
            seen_ids.add(icao)
            all_rows.append({
                "id": icao,                        # canonical ICAO (KALB / PANC)
                "iem_id": s["sid"],                # what IEM expects
                "name": s["name"],
                "state": s["state"],
                "country": s["country"],
                "lat": round(s["lat"], 4),
                "lon": round(s["lon"], 4),
                "elevation": s["elevation"],
                "network": s["network"],
                "online": s["online"],
                "archive_begin": s["archive_begin"],
                "archive_end": s["archive_end"],
            })

    all_rows.sort(key=lambda r: r["id"])

    out = Path(__file__).resolve().parent.parent / "asos_tools" / "data" / "stations.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(all_rows, indent=2))
    print(f"\nWrote {len(all_rows):,} stations to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
