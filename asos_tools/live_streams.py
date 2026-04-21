"""Unofficial live video streams (YouTube) for major airports.

Aviation spotters and airport-operated channels run 24/7 live video
feeds of a number of large airports on YouTube.  This module maps
ICAO identifiers to known live-stream sources so the station drill
panel can show a supplemental live-video embed beside the 5-min FAA
WeatherCam stills.

**None of these feeds are FAA-authoritative.**  They are spotter or
airport-PR streams; availability is best-effort and IDs go stale.
The UI surfaces every embed under a loud "UNOFFICIAL — not FAA
authoritative" label.

Two embed patterns are supported per entry:

  ``video_id``
      A specific YouTube video ID — stable as long as that video
      stays up.  Renders as ``https://www.youtube.com/embed/VIDEO_ID``.

  ``channel_id``
      A YouTube channel ID that broadcasts live regularly — resolves
      via ``https://www.youtube.com/embed/live_stream?channel=CHANNEL_ID``.
      If the channel is not currently live YouTube shows a graceful
      "channel is not live" placeholder instead of erroring.

Operators can override or add entries via the ``OWL_LIVE_STREAMS_JSON``
env var — a JSON object mapping ICAO → ``{"video_id": "..."}`` or
``{"channel_id": "..."}``.  Env entries win over the seed map.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "get_live_stream",
    "embed_url",
    "youtube_search_url",
    "LIVE_STREAMS_SEED",
]


# ---------------------------------------------------------------------------
# Seed map — kept intentionally small.  The UI always offers a YouTube
# search-link fallback for any airport not listed here, so "missing"
# entries still give the user a one-click path to live coverage.
#
# NOTE: YouTube channel/video IDs rot over time.  If an entry ever stops
# resolving, add it via OWL_LIVE_STREAMS_JSON or delete it from this map
# and the drill panel will fall back to the search-link UI automatically.
# ---------------------------------------------------------------------------
LIVE_STREAMS_SEED: dict[str, dict[str, Any]] = {
    # Major US hubs that have had long-running 24/7 spotter streams.
    # `channel_id` uses the `embed/live_stream?channel=...` URL pattern:
    # when that channel has a live broadcast, it auto-embeds; when it
    # doesn't, YouTube shows a friendly "not currently live" card
    # instead of breaking.  This is more durable than hard-coded
    # video IDs which rot as individual broadcasts end.
    #
    # `search` is the fallback text — if no channel/video is mapped
    # the drill panel renders a one-click YouTube live-now search.
    "KLAX": {
        # AirlineVideosLive+ — multiple concurrent 24/7 LAX streams,
        # verified actively broadcasting 2026-04-21.  When one stream
        # ends, YouTube rolls the next live broadcast on the channel.
        "channel_id": "UCox5yCEEjk4iYbhLgyj90EQ",
        "channel_name": "AirlineVideosLive+",
        "title": "LAX live spotter stream (AirlineVideosLive+)",
        "search": "LAX airport live camera",
    },
    "KJFK": {
        "title": "JFK – live spotter stream",
        "search": "JFK airport live camera",
    },
    "KBOS": {
        "title": "Boston Logan – live spotter stream",
        "search": "KBOS Logan live tower cam",
    },
    "KORD": {
        "title": "Chicago O'Hare – live spotter stream",
        "search": "ORD O'Hare live cam",
    },
    "KDFW": {
        "title": "Dallas/Fort Worth – live spotter stream",
        "search": "DFW airport live cam",
    },
    "KATL": {
        "title": "Atlanta Hartsfield – live spotter stream",
        "search": "Atlanta airport live cam",
    },
    "KDEN": {
        "title": "Denver International – live spotter stream",
        "search": "Denver airport live cam",
    },
    "KSEA": {
        "title": "Seattle-Tacoma – live spotter stream",
        "search": "Seattle airport live cam",
    },
    "KSFO": {
        "title": "San Francisco – live spotter stream",
        "search": "SFO airport live cam",
    },
    "KMIA": {
        "title": "Miami – live spotter stream",
        "search": "Miami airport live cam",
    },
    "KLAS": {
        "title": "Las Vegas – live spotter stream",
        "search": "LAS Harry Reid airport live cam",
    },
    "KPHX": {
        "title": "Phoenix Sky Harbor – live spotter stream",
        "search": "Phoenix airport live cam",
    },
    "KMCO": {
        "title": "Orlando MCO – live spotter stream",
        "search": "Orlando airport live cam",
    },
    "KDCA": {
        "title": "Reagan National DCA – live spotter stream",
        "search": "DCA airport live cam",
    },
    "KIAD": {
        "title": "Washington Dulles IAD – live spotter stream",
        "search": "Dulles airport live cam",
    },
    "KPHL": {
        "title": "Philadelphia PHL – live spotter stream",
        "search": "Philadelphia airport live cam",
    },
    "KIAH": {
        "title": "Houston IAH – live spotter stream",
        "search": "IAH Houston airport live cam",
    },
    "KDTW": {
        "title": "Detroit DTW – live spotter stream",
        "search": "DTW Detroit airport live cam",
    },
    "KMSP": {
        "title": "Minneapolis-St. Paul – live spotter stream",
        "search": "MSP airport live cam",
    },
    "KSAN": {
        "title": "San Diego – live spotter stream",
        "search": "San Diego airport live cam",
    },
}


def _load_env_overrides() -> dict[str, dict[str, Any]]:
    """Parse OWL_LIVE_STREAMS_JSON into a dict.  Silently no-op on bad JSON."""
    raw = (os.environ.get("OWL_LIVE_STREAMS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            logger.warning("OWL_LIVE_STREAMS_JSON is not a JSON object")
            return {}
        # Normalise keys to uppercase ICAO
        out: dict[str, dict[str, Any]] = {}
        for k, v in obj.items():
            if not isinstance(v, dict):
                continue
            out[str(k).strip().upper()] = v
        return out
    except Exception:
        logger.exception("OWL_LIVE_STREAMS_JSON parse failed")
        return {}


def get_live_stream(icao: str) -> Optional[dict[str, Any]]:
    """Return the live-stream config for an ICAO, or None if none known.

    Environment overrides (``OWL_LIVE_STREAMS_JSON``) win over the seed
    map.  Return shape (any subset of keys can appear):

        {
          "title":      "Human-friendly description",
          "video_id":   "YOUTUBE_VIDEO_ID",     # optional
          "channel_id": "YOUTUBE_CHANNEL_ID",   # optional
          "search":     "fallback search query" # optional
        }

    If neither ``video_id`` nor ``channel_id`` is present, callers
    should render the YouTube search-link UI instead of an iframe.
    """
    if not icao:
        return None
    key = icao.strip().upper()
    overrides = _load_env_overrides()
    if key in overrides:
        merged = {**LIVE_STREAMS_SEED.get(key, {}), **overrides[key]}
        return merged
    return LIVE_STREAMS_SEED.get(key)


def embed_url(cfg: dict[str, Any]) -> Optional[str]:
    """Build the YouTube embed URL for a live-stream config.

    Returns None if neither ``video_id`` nor ``channel_id`` is set —
    the caller should then fall back to the search-link UI.
    """
    vid = cfg.get("video_id")
    if vid:
        return f"https://www.youtube.com/embed/{vid}?autoplay=1&mute=1"
    ch = cfg.get("channel_id")
    if ch:
        return (
            f"https://www.youtube.com/embed/live_stream?channel={ch}"
            "&autoplay=1&mute=1"
        )
    return None


def youtube_search_url(query: str) -> str:
    """Build a YouTube live-search URL for a given query.

    Uses the ``sp=EgJAAQ%253D%253D`` filter which restricts results to
    currently-live broadcasts — so the user lands directly on real
    live streams for that airport instead of a mix of vods.
    """
    import urllib.parse as _up
    q = _up.quote_plus(query.strip())
    return f"https://www.youtube.com/results?search_query={q}&sp=EgJAAQ%253D%253D"
