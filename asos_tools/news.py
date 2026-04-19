"""NOAA / FAA / NTSB / AWC news + alert feed aggregator.

Aggregates ~8 public feeds into a single chronologically-sorted list
of ``{source, title, published, link, severity, relevance}`` dicts
suitable for a news ticker on the globe or a dedicated NEWS tab.

All feeds are fetched in parallel with a 10-second per-source timeout
and merged. Individual failures never raise — missing sources just drop
out of the result.

Uses ``feedparser`` for RSS/Atom and ``requests`` for JSON APIs
(AWC, NWS CAP) that don't publish standard RSS.
"""

from __future__ import annotations

import concurrent.futures
import logging
import re
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests

try:
    import feedparser
    _HAVE_FP = True
except ImportError:
    feedparser = None  # type: ignore
    _HAVE_FP = False

logger = logging.getLogger(__name__)

__all__ = [
    "fetch_noaa_faa_headlines",
    "SOURCES",
]


_UA = "O.W.L./1.0 (+github.com/consigcody94/asos-tools-py)"


#: Sources we aggregate. Each entry is (label, kind, url, severity_default).
SOURCES: list[dict] = [
    {"label": "NOAA", "kind": "rss",
     "url": "https://www.noaa.gov/feed/media-release",
     "severity": "info"},
    {"label": "NWS", "kind": "rss",
     "url": "https://www.weather.gov/news/rss_page",
     "severity": "info"},
    {"label": "FAA", "kind": "rss",
     "url": "https://www.faa.gov/newsroom/rss",
     "severity": "info"},
    {"label": "NTSB", "kind": "rss",
     "url": "https://www.ntsb.gov/rss/news.aspx",
     "severity": "info"},
    {"label": "AWC SIGMETs", "kind": "awc_sigmet",
     "url": "https://aviationweather.gov/api/data/airsigmet?format=json&age=2",
     "severity": "warning"},
    {"label": "NWS Alerts", "kind": "nws_cap",
     "url": "https://api.weather.gov/alerts/active?status=actual&urgency=Immediate,Expected",
     "severity": "warning"},
]


#: Relevance keywords boost a story's score in the merged feed.
_BOOST = re.compile(
    r"\b(ASOS|AWOS|METAR|TAF|NEXRAD|ceilometer|visibility|precipitation|"
    r"automated surface|FAA|NWS|NOAA|aviation|turbulence|icing|wind shear|"
    r"SIGMET|AIRMET|PIREP|CAT|thunderstorm|blizzard|hurricane|tornado)\b",
    re.IGNORECASE,
)


def _parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # Try RFC-822 / struct_time from feedparser
    try:
        return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %z")
    except Exception:
        return None


def _score(title: str, severity: str) -> float:
    """Higher score = more relevant to aviation weather ops."""
    base = {"warning": 3.0, "info": 1.0}.get(severity, 1.0)
    m = len(_BOOST.findall(title or "")) if title else 0
    return base + m


def _fetch_rss(src: dict) -> list[dict]:
    if not _HAVE_FP:
        return []
    try:
        parsed = feedparser.parse(src["url"], request_headers={"User-Agent": _UA})
        out = []
        for entry in parsed.entries[:25]:
            title = entry.get("title") or ""
            link = entry.get("link") or ""
            pub = entry.get("published") or entry.get("updated") or ""
            out.append({
                "source": src["label"],
                "title": title.strip(),
                "link": link,
                "published": _parse_iso(pub) or datetime.now(timezone.utc),
                "severity": src["severity"],
                "relevance": _score(title, src["severity"]),
            })
        return out
    except Exception:
        logger.exception("RSS fetch failed: %s", src["label"])
        return []


def _fetch_awc_sigmet(src: dict) -> list[dict]:
    try:
        r = requests.get(src["url"], timeout=10,
                         headers={"User-Agent": _UA})
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception("AWC SIGMET feed failed")
        return []
    out = []
    for item in (data or [])[:20]:
        hazard = item.get("hazard") or "SIGMET"
        area = item.get("area") or ""
        title = f"{hazard}: {area}".strip(": ")
        when = item.get("validTimeFrom") or ""
        out.append({
            "source": src["label"],
            "title": title,
            "link": "https://aviationweather.gov/gfa/#sigmet",
            "published": _parse_iso(when) or datetime.now(timezone.utc),
            "severity": "warning",
            "relevance": _score(title, "warning"),
        })
    return out


def _fetch_nws_cap(src: dict) -> list[dict]:
    try:
        r = requests.get(src["url"], timeout=10,
                         headers={"User-Agent": _UA, "Accept": "application/geo+json"})
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.exception("NWS CAP feed failed")
        return []
    out = []
    for feat in (data.get("features") or [])[:25]:
        props = feat.get("properties") or {}
        event = props.get("event") or "Alert"
        area = props.get("areaDesc") or ""
        title = f"{event}: {area}"[:200]
        sev_raw = (props.get("severity") or "").lower()
        sev = "warning" if sev_raw in ("extreme", "severe") else "info"
        when = props.get("sent") or props.get("effective") or ""
        out.append({
            "source": src["label"],
            "title": title,
            "link": props.get("uri") or "https://alerts.weather.gov",
            "published": _parse_iso(when) or datetime.now(timezone.utc),
            "severity": sev,
            "relevance": _score(title, sev),
        })
    return out


_FETCHERS = {
    "rss": _fetch_rss,
    "awc_sigmet": _fetch_awc_sigmet,
    "nws_cap": _fetch_nws_cap,
}


def fetch_noaa_faa_headlines(*, limit: int = 20,
                             only: Optional[Iterable[str]] = None,
                             sort: str = "time") -> list[dict]:
    """Return merged, deduplicated, sorted feed items.

    Parameters
    ----------
    limit
        Max items returned.
    only
        If given, keep only those source labels.
    sort
        "time" (newest first) or "relevance" (most relevant first).
    """
    sources = SOURCES
    if only:
        only_set = {s.upper() for s in only}
        sources = [s for s in SOURCES if s["label"].upper() in only_set]

    items: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = []
        for src in sources:
            fn = _FETCHERS.get(src["kind"])
            if fn:
                futures.append(ex.submit(fn, src))
        for f in concurrent.futures.as_completed(futures, timeout=30):
            try:
                items.extend(f.result() or [])
            except Exception:
                logger.exception("feed fetcher raised")

    # Deduplicate by (source, title) — feeds occasionally double-post.
    seen = set()
    deduped = []
    for i in items:
        key = (i.get("source"), i.get("title"))
        if key not in seen:
            seen.add(key)
            deduped.append(i)

    if sort == "relevance":
        deduped.sort(key=lambda i: (-i.get("relevance", 0), -i["published"].timestamp()))
    else:
        deduped.sort(key=lambda i: -i["published"].timestamp())

    return deduped[:limit]
