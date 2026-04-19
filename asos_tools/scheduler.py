"""APScheduler singleton for background watchlist refresh.

A ``BackgroundScheduler`` runs in the Streamlit process (one per server).
We use it to refresh the whole-network watchlist every 3 minutes, ahead
of user requests, so the Summary tab loads instantly.

Results are kept in the ``diskcache`` layer (see ``persistent_cache``).

Example use::

    from asos_tools.scheduler import get_scheduler, schedule_watchlist_refresh
    sched = get_scheduler()
    schedule_watchlist_refresh(sched, interval_minutes=3)

Background jobs respect the 15-min max window rule — they exit fast.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    _HAVE_APSCHEDULER = True
except ImportError:  # pragma: no cover
    BackgroundScheduler = None  # type: ignore
    IntervalTrigger = None  # type: ignore
    _HAVE_APSCHEDULER = False

logger = logging.getLogger(__name__)

__all__ = [
    "get_scheduler",
    "schedule_watchlist_refresh",
    "scheduler_status",
    "shutdown_scheduler",
]

_scheduler_lock = threading.Lock()
_scheduler: Optional["BackgroundScheduler"] = None
_last_run: dict[str, datetime] = {}
_last_error: dict[str, str] = {}


def get_scheduler() -> Optional["BackgroundScheduler"]:
    """Return the singleton scheduler, starting it lazily."""
    global _scheduler
    if not _HAVE_APSCHEDULER:
        return None
    with _scheduler_lock:
        if _scheduler is None:
            try:
                _scheduler = BackgroundScheduler(
                    timezone="UTC",
                    daemon=True,
                    job_defaults={
                        "coalesce": True,       # collapse missed runs
                        "max_instances": 1,     # no overlapping runs
                        "misfire_grace_time": 60,
                    },
                )
                _scheduler.start()
                logger.info("APScheduler started")
            except Exception:
                logger.exception("Failed to start APScheduler")
                _scheduler = None
        return _scheduler


def schedule_watchlist_refresh(
    scheduler: "BackgroundScheduler",
    refresh_fn: Callable[[], None],
    *,
    interval_minutes: int = 3,
    job_id: str = "watchlist_refresh",
) -> bool:
    """Register the watchlist refresh job. Idempotent."""
    if scheduler is None:
        return False

    def _wrapped():
        try:
            refresh_fn()
            _last_run[job_id] = datetime.now(timezone.utc)
            _last_error.pop(job_id, None)
        except Exception as e:
            logger.exception("Scheduled job %s failed", job_id)
            _last_error[job_id] = f"{type(e).__name__}: {e}"

    try:
        scheduler.add_job(
            _wrapped,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            replace_existing=True,
            next_run_time=datetime.now(timezone.utc),
        )
        return True
    except Exception:
        logger.exception("Failed to schedule job %s", job_id)
        return False


def scheduler_status() -> dict:
    """Return a summary dict of scheduler + job state for the Admin tab."""
    scheduler = _scheduler  # avoid holding lock during read
    if not _HAVE_APSCHEDULER:
        return {"available": False, "reason": "apscheduler not installed"}
    if scheduler is None:
        return {"available": True, "running": False, "jobs": []}
    jobs = []
    try:
        for j in scheduler.get_jobs():
            jobs.append({
                "id": j.id,
                "next_run": j.next_run_time.isoformat()
                    if j.next_run_time else None,
                "trigger": str(j.trigger),
                "last_run": _last_run.get(j.id).isoformat()
                    if _last_run.get(j.id) else None,
                "last_error": _last_error.get(j.id),
            })
    except Exception as e:
        logger.exception("Failed to introspect scheduler")
        return {"available": True, "running": False, "error": str(e), "jobs": []}
    return {
        "available": True,
        "running": scheduler.running,
        "jobs": jobs,
    }


def shutdown_scheduler() -> None:
    """Gracefully stop the scheduler (called on app shutdown)."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler is not None:
            try:
                _scheduler.shutdown(wait=False)
            except Exception:
                logger.exception("Scheduler shutdown raised")
            _scheduler = None
