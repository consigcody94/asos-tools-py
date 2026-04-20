"""Preconfigured ASOS station groups.

These groups were curated by the original ``dmhuehol/ASOS-Tools`` project
(North Carolina State University / Environment Analytics) and are reproduced
here with a few additions for major hubs. They're useful as ready-made
inputs to :func:`asos_tools.fetch_1min` and :func:`asos_tools.fetch_metars`.

Example
-------
>>> from asos_tools import fetch_1min
>>> from asos_tools.stations import LONG_ISLAND, get_group
>>> df = fetch_1min(LONG_ISLAND, t0, t1)
>>> df = fetch_1min(get_group("front_range"), t0, t1)
"""

from __future__ import annotations

from typing import Tuple

import json
from functools import lru_cache
from pathlib import Path

__all__ = [
    "LONG_ISLAND",
    "NORTHEAST",
    "CO_LOCATED_RADIOSONDE",
    "FRONT_RANGE",
    "UTAH",
    "MAJOR_HUBS",
    "COASTAL_EAST",
    "GREAT_LAKES",
    "GULF_COAST",
    "ALASKA_PACIFIC",
    "GROUPS",
    "get_group",
    "list_groups",
    "all_stations",
    "ALL_ASOS_STATIONS",
    "search_stations",
    "stations_by_state",
    "AOMC_STATIONS",
    "AOMC_IDS",
    "is_aomc",
]


# --- Curated groups -------------------------------------------------------

#: Stations around Long Island, NY (Stony Brook-area focus).
LONG_ISLAND: Tuple[str, ...] = (
    "KISP",  # Islip -- closest to Stony Brook University
    "KHWV",  # Brookhaven
    "KFRG",  # Farmingdale
    "KFOK",  # Westhampton
    "KJFK",  # JFK airport
    "KLGA",  # LaGuardia
    "KEWR",  # Newark
    "KTEB",  # Teterboro
)

#: Broader Northeast US.
NORTHEAST: Tuple[str, ...] = (
    # Connecticut
    "KHVN", "KBDR", "KGON",
    # New Jersey
    "KVAY", "KTTN", "KCDW", "KSMQ", "KACY",
    # New York
    "KHPN", "KFWN", "KPOU", "KMGJ",
    # Rhode Island
    "KWST",
)

#: ASOS stations co-located with radiosonde (upper-air) launch sites.
CO_LOCATED_RADIOSONDE: Tuple[str, ...] = (
    "KGSO",  # Greensboro, NC (GSO)
    "KFFC",  # Peachtree City, GA (FFC)
    "KALB",  # Albany, NY (ALB)
    "KDET",  # Detroit/White Lake, MI (DTX)
    "KCAR",  # Caribou, ME (CAR)
    "KHQM",  # Quillayute, WA (UIL)
    "KBIS",  # Bismarck, ND (BIS)
)

#: Front Range / Colorado-Wyoming corridor.
FRONT_RANGE: Tuple[str, ...] = (
    "KAPA",  # Denver - Centennial, CO
    "KDEN",  # Denver International, CO
    "KCYS",  # Cheyenne, WY
    "KLAR",  # Laramie, WY
)

#: Utah (Wasatch front).
UTAH: Tuple[str, ...] = (
    "KLGU",  # Logan
    "KOGD",  # Ogden
    "KSLC",  # Salt Lake City (closest to Alta)
)

#: Major US passenger airport hubs.
MAJOR_HUBS: Tuple[str, ...] = (
    "KATL",  # Atlanta
    "KBOS",  # Boston
    "KCLT",  # Charlotte
    "KDEN",  # Denver
    "KDFW",  # Dallas/Fort Worth
    "KIAD",  # Washington Dulles
    "KIAH",  # Houston
    "KJFK",  # New York JFK
    "KLAS",  # Las Vegas
    "KLAX",  # Los Angeles
    "KMCO",  # Orlando
    "KMIA",  # Miami
    "KMSP",  # Minneapolis
    "KORD",  # Chicago O'Hare
    "KPHX",  # Phoenix
    "KSEA",  # Seattle-Tacoma
    "KSFO",  # San Francisco
)

#: US East Coast / mid-Atlantic coastal stations (useful for nor'easters).
COASTAL_EAST: Tuple[str, ...] = (
    "KBOS", "KPVD", "KHPN", "KJFK", "KLGA", "KEWR",
    "KACY", "KPHL", "KDOV", "KDCA", "KBWI", "KNFK",
    "KRDU", "KILM", "KCHS", "KSAV", "KJAX",
)

#: Great Lakes.
GREAT_LAKES: Tuple[str, ...] = (
    "KORD", "KMKE", "KGRR", "KDET", "KCLE", "KBUF", "KROC",
    "KERI", "KSYR", "KDTW",
)

#: Gulf Coast.
GULF_COAST: Tuple[str, ...] = (
    "KHOU", "KIAH", "KLCH", "KMSY", "KBIX", "KMOB", "KPNS",
    "KPIE", "KTPA", "KRSW",
)

#: Alaska + Pacific territories (note: no K-prefix).
ALASKA_PACIFIC: Tuple[str, ...] = (
    "PANC",  # Anchorage
    "PAFA",  # Fairbanks
    "PAJN",  # Juneau
    "PHNL",  # Honolulu
    "PHOG",  # Kahului, Maui
    "PHTO",  # Hilo
    "PGUM",  # Guam
)


GROUPS: dict[str, Tuple[str, ...]] = {
    "long_island": LONG_ISLAND,
    "northeast": NORTHEAST,
    "co_located_radiosonde": CO_LOCATED_RADIOSONDE,
    "front_range": FRONT_RANGE,
    "utah": UTAH,
    "major_hubs": MAJOR_HUBS,
    "coastal_east": COASTAL_EAST,
    "great_lakes": GREAT_LAKES,
    "gulf_coast": GULF_COAST,
    "alaska_pacific": ALASKA_PACIFIC,
}


def get_group(name: str) -> Tuple[str, ...]:
    """Look up a station group by case-insensitive name."""
    key = name.strip().lower().replace("-", "_").replace(" ", "_")
    if key not in GROUPS:
        raise KeyError(
            f"Unknown station group {name!r}. "
            f"Available: {', '.join(sorted(GROUPS))}"
        )
    return GROUPS[key]


def list_groups() -> list[str]:
    """Return the names of all known groups, sorted."""
    return sorted(GROUPS)


def all_stations() -> Tuple[str, ...]:
    """Union of every preset station across all groups (deduped, sorted)."""
    seen: dict[str, None] = {}
    for stations in GROUPS.values():
        for s in stations:
            seen[s] = None
    return tuple(sorted(seen))


# ---------------------------------------------------------------------------
# Bundled full ASOS catalog (2,900+ US sites + territories)
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).resolve().parent / "data"
_CATALOG_PATH = _DATA_DIR / "stations.json"


@lru_cache(maxsize=1)
def _load_catalog() -> list[dict]:
    """Load the bundled ASOS station catalog. Empty list if not built yet."""
    if not _CATALOG_PATH.exists():
        return []
    try:
        return json.loads(_CATALOG_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return []


# Module-level constant — loaded once, immutable for callers' purposes.
ALL_ASOS_STATIONS: list[dict] = _load_catalog()


def search_stations(
    query: str = "",
    *,
    state: str | None = None,
    online_only: bool = False,
    limit: int | None = None,
) -> list[dict]:
    """Search the bundled ASOS catalog.

    Parameters
    ----------
    query
        Case-insensitive substring match against ``id`` and ``name``.
        Empty string matches everything.
    state
        Optional two-letter state / territory filter (e.g. ``"NY"``).
    online_only
        If True, only return stations IEM currently lists as online.
    limit
        Optional maximum number of results to return.

    Returns
    -------
    list of dict
        Station records ordered so exact-``id`` matches come first, then
        prefix matches, then substring matches.
    """
    rows = ALL_ASOS_STATIONS
    if not rows:
        return []

    if state:
        state_u = state.upper()
        rows = [r for r in rows if (r.get("state") or "").upper() == state_u]
    if online_only:
        rows = [r for r in rows if r.get("online")]

    q = (query or "").strip().upper()
    if not q:
        return rows[:limit] if limit else list(rows)

    def _score(r: dict) -> int:
        """Rank results: exact ID = 0, prefix match on ID = 1, substring ID = 2,
        exact FAA LID = 3, exact IATA = 4, name prefix = 5, name substring = 6,
        WMO = 7, state match = 8.  A user typing 'ANC' finds PANC, 'HNL' finds
        PHNL, 'JFK' finds KJFK, 'PANC' finds PANC — no guessing which prefix."""
        rid   = (r.get("id")        or "").upper()
        call  = (r.get("call")      or "").upper()
        iata  = (r.get("iata")      or "").upper()
        wmo   = str(r.get("wmo")    or "").upper()
        name  = (r.get("name")      or "").upper()
        alt   = (r.get("alt_name")  or "").upper()
        state = (r.get("state")     or "").upper()

        # Exact-ID family first (most common).
        if rid == q:
            return 0
        # Exact FAA LID or IATA — user typed the 3-letter airport code.
        if q == call or q == iata:
            return 1
        # Prefix match on ID ("PA" -> all Alaska, "K" -> all CONUS).
        if rid.startswith(q):
            return 2
        # Substring ID ("NC" -> KCLT, PANC, etc.).
        if q in rid:
            return 3
        # FAA / IATA partial.
        if call.startswith(q) or iata.startswith(q):
            return 4
        if q == wmo:
            return 5
        # Name prefix / substring / alt name.
        if name.startswith(q):
            return 6
        if q in name:
            return 7
        if alt and (alt.startswith(q) or q in alt):
            return 8
        # State-code hit as last resort ("CA" -> all California stations).
        if state == q and len(q) == 2:
            return 9
        return 99

    matched = [(r, _score(r)) for r in rows]
    matched = [(r, s) for r, s in matched if s < 99]
    matched.sort(key=lambda x: (x[1], x[0].get("id", "")))
    out = [r for r, _ in matched]
    return out[:limit] if limit else out


def stations_by_state(state: str) -> list[dict]:
    """All catalog stations in a given state/territory (two-letter code)."""
    return search_stations(state=state)


# ---------------------------------------------------------------------------
# AOMC (ASOS Operations and Monitoring Center) — federal ASOS list
# ---------------------------------------------------------------------------

_AOMC_PATH = _DATA_DIR / "aomc_stations.json"


@lru_cache(maxsize=1)
def _load_aomc() -> dict:
    """Load the bundled AOMC station list. Empty payload if file missing."""
    if not _AOMC_PATH.exists():
        return {"stations": [], "source": "", "fetched_utc": "", "record_count": 0}
    try:
        return json.loads(_AOMC_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"stations": [], "source": "", "fetched_utc": "", "record_count": 0}


_aomc_payload = _load_aomc()

#: Full list of AOMC-certified ASOS stations (~920 sites) as dicts, from
#: NCEI HOMR's authoritative asos-stations.txt. Each record has id, call,
#: wban, coop_id, ghcnd_id, name, state, county, lat, lon, elev_ft,
#: utc_offset_hr, station_types (e.g. "AIRWAYS,ASOS,COOP"), begin_date.
AOMC_STATIONS: list[dict] = list(_aomc_payload.get("stations", []))

#: Set of canonical ICAO ids for all AOMC ASOS stations — for quick O(1)
#: membership checks against the broader IEM catalog.
AOMC_IDS: frozenset[str] = frozenset(s["id"] for s in AOMC_STATIONS if s.get("id"))


def is_aomc(station_id: str) -> bool:
    """True if this ICAO id is on the federal AOMC ASOS list."""
    if not station_id:
        return False
    return station_id.strip().upper() in AOMC_IDS
