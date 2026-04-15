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
