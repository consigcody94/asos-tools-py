"""Source-of-truth registry for every upstream data feed O.W.L. uses.

Surfaced on the Admin tab so operators can see where each number came
from, what the refresh cadence is, and whether the source is federal
authoritative or a mirror/aggregator.
"""

from __future__ import annotations

from typing import TypedDict

__all__ = ["SOURCES", "Source"]


class Source(TypedDict):
    name: str
    url: str
    used_for: str
    auth: str
    cadence: str
    trust: str     # federal | mirror | aggregator | crowdsourced
    notes: str


SOURCES: list[Source] = [
    {
        "name": "Iowa Environmental Mesonet (IEM)",
        "url": "https://mesonet.agron.iastate.edu",
        "used_for": "Primary METAR + 1-minute ASOS fetch",
        "auth": "none",
        "cadence": "near real-time",
        "trust": "mirror",
        "notes": ("Academic mirror of NCEI / NOAA archives. Free, fast, "
                  "no API key. Occasionally 5xxs under load."),
    },
    {
        "name": "NOAA NCEI Access Services",
        "url": "https://www.ncei.noaa.gov/access/services/data/v1",
        "used_for": "Fallback when IEM is unavailable",
        "auth": "none",
        "cadence": "hourly",
        "trust": "federal",
        "notes": "Authoritative federal archive. Slower than IEM.",
    },
    {
        "name": "NWS api.weather.gov",
        "url": "https://api.weather.gov",
        "used_for": "Current conditions + active CAP alerts",
        "auth": "none (UA required)",
        "cadence": "real-time",
        "trust": "federal",
        "notes": ("National Weather Service public API. Requires a "
                  "descriptive User-Agent."),
    },
    {
        "name": "Aviation Weather Center (AWC)",
        "url": "https://aviationweather.gov/api/data",
        "used_for": "METAR, TAF, SIGMET, AIRMET, PIREP, AFD",
        "auth": "none",
        "cadence": "real-time",
        "trust": "federal",
        "notes": "FAA-supported public API. Powers Forecasters tab.",
    },
    {
        "name": "FAA WeatherCams",
        "url": "https://weathercams.faa.gov",
        "used_for": "Live airport webcam still images (10-min refresh)",
        "auth": "none",
        "cadence": "10 min",
        "trust": "federal",
        "notes": ("260 FAA + 530 hosted camera sites, CONUS / Alaska / "
                  "Hawaii / Canada. Images only, no video streams."),
    },
    {
        "name": "NOAA Media Release RSS",
        "url": "https://www.noaa.gov/feed/media-release",
        "used_for": "News ticker",
        "auth": "none",
        "cadence": "as-published",
        "trust": "federal",
        "notes": "",
    },
    {
        "name": "FAA Newsroom RSS",
        "url": "https://www.faa.gov/newsroom/rss",
        "used_for": "News ticker",
        "auth": "none",
        "cadence": "as-published",
        "trust": "federal",
        "notes": "",
    },
    {
        "name": "NTSB Aviation Investigations RSS",
        "url": "https://www.ntsb.gov/rss/news.aspx",
        "used_for": "News ticker",
        "auth": "none",
        "cadence": "as-published",
        "trust": "federal",
        "notes": "",
    },
    {
        "name": "NCEI HOMR (station catalog)",
        "url": "https://www.ncei.noaa.gov/homr",
        "used_for": "AOMC station metadata (920 stations)",
        "auth": "none",
        "cadence": "static (baked into repo)",
        "trust": "federal",
        "notes": "asos-stations.txt snapshot; refreshed quarterly.",
    },
    {
        "name": "FAA NOTAM API",
        "url": "https://external-api.faa.gov/notamapi/v1/notams",
        "used_for": "Optional planned-outage correlation",
        "auth": "API key (FAA_NOTAM_KEY env var)",
        "cadence": "real-time",
        "trust": "federal",
        "notes": "Requires free registration.",
    },
]
