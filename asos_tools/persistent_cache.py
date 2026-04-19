"""Persistent disk cache for O.W.L. — survives Space restarts.

Wraps ``diskcache.Cache`` with a small, typed API:

- :func:`get_cache`           singleton cache instance
- :func:`put_watchlist`       store a watchlist scan by cache key
- :func:`get_watchlist`       retrieve if still fresh
- :func:`cache_stats`         size + hit rate summary for the Admin tab

Cache directory is ``~/.owl-cache`` by default; override with the
``OWL_CACHE_DIR`` env var. Maximum size: 200 MB (plenty for watchlist
scans + 1-min fetches).
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import diskcache
    _HAVE_DISKCACHE = True
except ImportError:  # pragma: no cover
    diskcache = None  # type: ignore
    _HAVE_DISKCACHE = False

logger = logging.getLogger(__name__)

__all__ = [
    "get_cache",
    "put_watchlist",
    "get_watchlist",
    "cache_stats",
    "clear_cache",
]

_cache_lock = threading.Lock()
_cache: Optional["diskcache.Cache"] = None
_hits = 0
_misses = 0


def _cache_dir() -> Path:
    raw = os.environ.get("OWL_CACHE_DIR")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".owl-cache"


def get_cache() -> Optional["diskcache.Cache"]:
    global _cache
    if not _HAVE_DISKCACHE:
        return None
    with _cache_lock:
        if _cache is None:
            try:
                d = _cache_dir()
                d.mkdir(parents=True, exist_ok=True)
                _cache = diskcache.Cache(
                    str(d),
                    size_limit=200 * 1024 * 1024,   # 200 MB
                    eviction_policy="least-recently-used",
                )
                logger.info("diskcache opened at %s", d)
            except Exception:
                logger.exception("Failed to open diskcache")
                _cache = None
    return _cache


def put_watchlist(cache_key: str, value: Any, *, ttl_seconds: int = 300) -> bool:
    """Store a watchlist DataFrame (or any picklable object)."""
    c = get_cache()
    if c is None:
        return False
    try:
        c.set(f"wl:{cache_key}", value, expire=ttl_seconds)
        return True
    except Exception:
        logger.exception("diskcache put failed")
        return False


def get_watchlist(cache_key: str) -> Optional[Any]:
    """Retrieve a cached watchlist; None if expired/missing."""
    global _hits, _misses
    c = get_cache()
    if c is None:
        _misses += 1
        return None
    try:
        v = c.get(f"wl:{cache_key}")
        if v is None:
            _misses += 1
        else:
            _hits += 1
        return v
    except Exception:
        logger.exception("diskcache get failed")
        _misses += 1
        return None


def cache_stats() -> dict:
    """Return a summary for the Admin tab."""
    c = get_cache()
    if c is None:
        return {"available": False, "reason": "diskcache unavailable"}
    try:
        size_bytes = c.volume()
        count = len(c)
        return {
            "available": True,
            "directory": str(_cache_dir()),
            "items": count,
            "size_mb": size_bytes / (1024 * 1024),
            "hits": _hits,
            "misses": _misses,
            "hit_rate": (100.0 * _hits / (_hits + _misses)) if (_hits + _misses) else 0.0,
            "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    except Exception as e:
        logger.exception("cache_stats failed")
        return {"available": True, "error": str(e)}


def clear_cache() -> int:
    """Wipe the cache. Returns number of items evicted."""
    c = get_cache()
    if c is None:
        return 0
    try:
        n = len(c)
        c.clear()
        return n
    except Exception:
        logger.exception("cache clear failed")
        return 0
