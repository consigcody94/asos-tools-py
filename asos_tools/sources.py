"""Source-of-truth registry for every upstream data feed O.W.L. uses.

Surfaced on the Admin tab so operators can see where each number came
from, what the refresh cadence is, and whether the source is an agency
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
    trust: str     # agency | mirror | aggregator | crowdsourced
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
        "trust": "agency",
        "notes": "Authoritative NCEI archive. Slower than IEM.",
    },
    {
        "name": "NWS api.weather.gov",
        "url": "https://api.weather.gov",
        "used_for": "Current conditions + active CAP alerts",
        "auth": "none (UA required)",
        "cadence": "real-time",
        "trust": "agency",
        "notes": ("National Weather Service public API. Requires a "
                  "descriptive User-Agent."),
    },
    {
        "name": "Aviation Weather Center (AWC)",
        "url": "https://aviationweather.gov/api/data",
        "used_for": "METAR, TAF, SIGMET, AIRMET, PIREP, AFD",
        "auth": "none",
        "cadence": "real-time",
        "trust": "agency",
        "notes": "FAA-supported public API. Powers Forecasters tab.",
    },
    {
        "name": "NWS RIDGE NEXRAD",
        "url": "https://radar.weather.gov/ridge/standard",
        "used_for": "Per-station WSR-88D base-reflectivity animated loops",
        "auth": "none",
        "cadence": "5 min",
        "trust": "agency",
        "notes": ("159 WSR-88D sites bundled at `data/wsr88d_sites.json`. "
                  "Nearest-neighbor pick; CONUS composite fallback."),
    },
    {
        "name": "NESDIS GOES-19",
        "url": "https://cdn.star.nesdis.noaa.gov/GOES19",
        "used_for": "GOES-19 CONUS + sector animated GIF loops, latest stills",
        "auth": "none",
        "cadence": "5 min (CONUS), 1 min (MESO)",
        "trust": "agency",
        "notes": ("Pre-rendered loops per sector (NE/SE/UMV/SMV/NR/SR/"
                  "PR/SP/CONUS). Zero-auth, CSP-allowed."),
    },
    {
        "name": "NESDIS GOES-18 West",
        "url": "https://cdn.star.nesdis.noaa.gov/GOES18",
        "used_for": "GOES-18 AK / HI / PNW / PSW sector loops (west Pacific coverage)",
        "auth": "none",
        "cadence": "5 min (CONUS), 1 min (MESO)",
        "trust": "agency",
        "notes": ("Alaska sector ships at 1000x1000; HI/PNW/PSW at 600x600. "
                  "Used for stations GOES-19 undercovers."),
    },
    {
        "name": "USGS Earthquake Hazards",
        "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary",
        "used_for": "Real-time quake feeds — site-proximity correlation for sensor dropouts",
        "auth": "none",
        "cadence": "~1 min",
        "trust": "agency",
        "notes": ("GeoJSON summary feeds: all_hour, 2.5_day, all_day, "
                  "2.5_week, significant_week. CC0 data."),
    },
    {
        "name": "NOAA NHC CurrentStorms",
        "url": "https://www.nhc.noaa.gov/CurrentStorms.json",
        "used_for": "Active tropical cyclones (Atlantic + East/Central Pacific)",
        "auth": "none",
        "cadence": "as advisories issue",
        "trust": "agency",
        "notes": ("Empty in off-season. Records include ATCF ID, position, "
                  "intensity, cone/track shapefile URLs, public advisory link."),
    },
    {
        "name": "NOAA NDBC buoys",
        "url": "https://www.ndbc.noaa.gov/data/realtime2",
        "used_for": "Marine met obs — coastal ASOS cross-check (wind / pressure / temp)",
        "auth": "none",
        "cadence": "10–30 min per station",
        "trust": "agency",
        "notes": ("~402 met-enabled buoys + CMAN stations bundled at "
                  "`data/ndbc_met_stations.json`."),
    },
    {
        "name": "AWC fcstdisc (AFD)",
        "url": "https://aviationweather.gov/api/data/fcstdisc",
        "used_for": "Area Forecast Discussion — WFO forecaster narrative",
        "auth": "none",
        "cadence": "~3x daily per WFO",
        "trust": "agency",
        "notes": ("Text/plain response; requires 4-letter ICAO CWA. Surfaced "
                  "in Forecasters -> Regional Discussion sub-tab."),
    },
    {
        "name": "FAA WeatherCams",
        "url": "https://weathercams.faa.gov",
        "used_for": "Live airport webcam still images (10-min refresh)",
        "auth": "none",
        "cadence": "10 min",
        "trust": "agency",
        "notes": ("260 FAA + 530 hosted camera sites, CONUS / Alaska / "
                  "Hawaii / Canada. Images only, no video streams."),
    },
    {
        "name": "NOAA Media Release RSS",
        "url": "https://www.noaa.gov/feed/media-release",
        "used_for": "News ticker",
        "auth": "none",
        "cadence": "as-published",
        "trust": "agency",
        "notes": "",
    },
    {
        "name": "FAA Newsroom RSS",
        "url": "https://www.faa.gov/newsroom/rss",
        "used_for": "News ticker",
        "auth": "none",
        "cadence": "as-published",
        "trust": "agency",
        "notes": "",
    },
    {
        "name": "NTSB Aviation Investigations RSS",
        "url": "https://www.ntsb.gov/rss/news.aspx",
        "used_for": "News ticker",
        "auth": "none",
        "cadence": "as-published",
        "trust": "agency",
        "notes": "",
    },
    {
        "name": "NCEI HOMR (station catalog)",
        "url": "https://www.ncei.noaa.gov/homr",
        "used_for": "AOMC station metadata (920 stations)",
        "auth": "none",
        "cadence": "static (baked into repo)",
        "trust": "agency",
        "notes": "asos-stations.txt snapshot; refreshed quarterly.",
    },
    {
        "name": "FAA NOTAM API",
        "url": "https://external-api.faa.gov/notamapi/v1/notams",
        "used_for": "Per-station NOTAM correlation for MISSING / FLAGGED sites",
        "auth": "client_id + client_secret (FAA_NOTAM_CLIENT_ID / FAA_NOTAM_CLIENT_SECRET)",
        "cadence": "real-time",
        "trust": "agency",
        "notes": ("Optional source — surfaced in drill-panel Site Hazards "
                  "when configured, otherwise shows a 'configure credentials' "
                  "hint. Free registration at FAA developer portal."),
    },
]
