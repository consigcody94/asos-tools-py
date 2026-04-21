"""O.W.L. FastAPI service — REST endpoints + scheduled-scan webhook.

Runs alongside Streamlit inside the same HF Spaces container, reached via
nginx at ``/api/*``. Purpose:

* **Replace in-process APScheduler** with an externally-triggered webhook.
  A GitHub Actions scheduled workflow (free, rock-solid, independent of the
  container's lifecycle) POSTs to ``/api/tick`` every 5 minutes. The handler
  runs the watchlist scan, dispatches Apprise alerts, persists the result.
  If HF restarts the container, the next cron tick picks up seamlessly —
  no lost scheduler state.

* **Expose lightweight REST** that the Streamlit UI (and later the globe
  JS) can hit for JSON-returning helpers that would otherwise need a full
  Streamlit rerun.

* **Surface health** for monitoring / UptimeRobot ping.

Endpoints
---------
``GET  /api/health``        -> service + data-freshness status
``POST /api/tick``          -> triggers a scan, requires ``X-OWL-Secret``
``GET  /api/sources``       -> data source registry
``GET  /api/webcams/near``  -> nearest FAA webcams for a lat/lon

All responses are JSON. Auth on ``/tick`` is a shared secret in an env var
(``OWL_CRON_SECRET``); if unset, the endpoint 401s every caller so a
misconfigured deploy can't accidentally permit anonymous scans.
"""

from __future__ import annotations

import hmac
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

# Switch all logging to structured JSON — one line per record, easy to
# grep via `jq` and parse by external log aggregators.
from asos_tools.logging_ext import install_json_logging, log_event
install_json_logging()
_log = logging.getLogger("owl.api")

app = FastAPI(
    title="O.W.L. REST",
    description="Observation Watch Log internal REST service.",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url=None,
    openapi_url="/api/openapi.json",
)

# Shared state — last tick result, last error. In-memory only; persistence
# lives in the DiskCache that the Streamlit side already writes to.
# Serializes every read-modify-write against _STATE.  BackgroundTasks
# runs _run_scan() on a worker thread that executes concurrently with
# request handlers on the event loop; without a lock, the check-then-
# enqueue in tick() is a TOCTOU and the duration-percentile bookkeeping
# at the tail of _run_scan corrupts the rolling list on concurrent ticks.
_STATE_LOCK = threading.Lock()

_STATE: dict[str, Any] = {
    "boot_time": datetime.now(timezone.utc).isoformat(),
    "last_tick_at": None,
    "last_tick_ok": None,
    "last_tick_stations": 0,
    "last_tick_flagged": 0,
    "last_tick_duration_s": None,
    "last_error": None,
    # --- cumulative metrics -------------------------------------------
    "tick_count_total": 0,           # cron ticks accepted since boot
    "tick_count_ok": 0,              # ticks that completed without exc
    "tick_count_failed": 0,          # ticks that raised
    "tick_count_skipped_overlap": 0, # ticks skipped because one in flight
    "tick_p50_duration_s": None,
    "tick_p95_duration_s": None,
    "tick_durations": [],            # rolling window, last ~50
}


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------
def _check_secret(header_value: str | None) -> None:
    expected = os.environ.get("OWL_CRON_SECRET", "")
    if not expected:
        _log.warning("OWL_CRON_SECRET unset - rejecting /api/tick")
        raise HTTPException(status_code=401, detail="cron secret not configured")
    if not header_value or not hmac.compare_digest(header_value, expected):
        raise HTTPException(status_code=401, detail="invalid cron secret")


# ---------------------------------------------------------------------------
# GET /api/health
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health() -> dict[str, Any]:
    """Service liveness + last-tick freshness. Hit by UptimeRobot / CI."""
    now = datetime.now(timezone.utc)
    last = _STATE.get("last_tick_at")
    stale = True
    if last:
        last_dt = datetime.fromisoformat(last)
        stale = (now - last_dt) > timedelta(minutes=15)

    return {
        "status": "ok",
        "now": now.isoformat(),
        "boot_time": _STATE["boot_time"],
        "last_tick_at": _STATE["last_tick_at"],
        "last_tick_ok": _STATE["last_tick_ok"],
        "last_tick_stations": _STATE["last_tick_stations"],
        "last_tick_flagged": _STATE["last_tick_flagged"],
        "last_tick_duration_s": _STATE["last_tick_duration_s"],
        "last_error": _STATE["last_error"],
        "data_stale": stale,
        "cache_dir": os.environ.get("OWL_CACHE_DIR", "(unset)"),
        "scan_in_flight": bool(_STATE.get("scan_in_flight", False)),
        "status_counts": _STATE.get("status_counts", {}),
        "upstream_outage": bool(_STATE.get("upstream_outage", False)),
        "tick_counts": {
            "total":           int(_STATE.get("tick_count_total", 0)),
            "ok":              int(_STATE.get("tick_count_ok", 0)),
            "failed":          int(_STATE.get("tick_count_failed", 0)),
            "skipped_overlap": int(_STATE.get("tick_count_skipped_overlap", 0)),
        },
        "tick_p50_duration_s": _STATE.get("tick_p50_duration_s"),
        "tick_p95_duration_s": _STATE.get("tick_p95_duration_s"),
    }


# ---------------------------------------------------------------------------
# POST /api/tick — fire-and-forget background scan
# ---------------------------------------------------------------------------
def _run_scan() -> None:
    """The actual scan body, run asynchronously by FastAPI BackgroundTasks.

    Stays out of the request/response cycle so nginx never times out a
    cron POST.  Result is stored in ``_STATE`` and surfaced via /api/health.
    """
    start = time.perf_counter()
    now = datetime.now(timezone.utc)
    # scan_in_flight + scan_started_at were already set under the lock
    # by tick() before queueing this task.  Pre-initialize duration so
    # the `finally` block can reference it even if the try raises before
    # the assignment below.
    duration = 0.0

    try:
        # Import lazily so the FastAPI worker boots even if a data module
        # is temporarily broken — health still responds, tick returns an
        # explicit error payload.
        from asos_tools.stations import AOMC_STATIONS
        from asos_tools.watchlist import build_watchlist

        hours = float(os.environ.get("OWL_SCAN_HOURS", "4"))

        # Pass the full AOMC dicts so build_watchlist can enrich
        # rows with name/state for the alert body.
        wl = build_watchlist(AOMC_STATIONS, hours=hours, end=now)

        # Status enum (from watchlist.STATUS_ORDER):
        #   MISSING, FLAGGED, INTERMITTENT  -> needs attention
        #   RECOVERED, CLEAN                 -> healthy
        #   NO DATA                          -> uncategorized
        flagged = 0
        status_counts: dict[str, int] = {}
        attention_states = {"MISSING", "FLAGGED", "INTERMITTENT"}
        if wl is not None and not wl.empty and "status" in wl.columns:
            # Normalize just in case the enum ever drifts in case.
            s = wl["status"].astype(str).str.upper().str.strip()
            flagged = int(s.isin(attention_states).sum())
            # Full histogram for debugging via /api/health.
            status_counts = {k: int(v) for k, v in s.value_counts().items()}
        _STATE["status_counts"] = status_counts

        # ---- Circuit breaker: detect IEM-wide outage ------------------
        # If EVERY station classified as MISSING, the upstream (IEM)
        # returned nothing — typically because we were rate-limited (429)
        # or IEM was 503-ing.  Don't dispatch 920 MISSING alerts in that
        # case, and flag the health endpoint so operators can see why.
        all_missing = (
            len(status_counts) == 1
            and "MISSING" in status_counts
            and status_counts["MISSING"] >= 50  # guard against genuinely-dark small deployments
        )
        if all_missing:
            _log.warning(
                "scan returned 100%% MISSING - treating as upstream outage "
                "(IEM rate-limited or 503). Suppressing %d MISSING alerts.",
                status_counts["MISSING"],
            )
            _STATE["upstream_outage"] = True
            # Replace the bogus flagged count with zero so downstream
            # dashboards don't light up red on a data-source problem.
            flagged = 0
        else:
            _STATE["upstream_outage"] = False

        # Fire Apprise notifications if configured + severity warrants.
        # Dispatch one alert per newly-flagged station; missing-data rows
        # get a distinct MISSING alert. Both are no-ops if OWL_ALERT_URLS
        # isn't set, so this is safe to call unconditionally.
        alerts_sent = 0
        try:
            from asos_tools.alerts import (
                load_urls_from_env,
                send_flag_alert,
                send_missing_alert,
            )

            urls = load_urls_from_env()
            # Skip the alert loop entirely if we've detected an upstream
            # outage — the MISSING classifications are bogus in that state.
            if (urls and wl is not None and not wl.empty
                    and "status" in wl.columns
                    and not _STATE.get("upstream_outage", False)):
                for _, row in wl.iterrows():
                    status = str(row.get("status", "")).upper()
                    if status == "FLAGGED":
                        sent, _ = send_flag_alert(row.to_dict(), urls=urls)
                        alerts_sent += sent
                    elif status == "MISSING":
                        sent, _ = send_missing_alert(row.to_dict(), urls=urls)
                        alerts_sent += sent
        except Exception as e:  # noqa: BLE001
            _log.warning("alert dispatch failed: %s", e)

        duration = time.perf_counter() - start

        _STATE.update(
            {
                "last_tick_at": now.isoformat(),
                "last_tick_ok": True,
                "last_tick_stations": int(len(wl)) if wl is not None else 0,
                "last_tick_flagged": flagged,
                "last_tick_duration_s": round(duration, 2),
                "last_error": None,
            }
        )

        log_event(
            "scan.ok",
            stations=_STATE["last_tick_stations"],
            flagged=flagged,
            alerts=alerts_sent,
            duration_s=round(duration, 2),
            upstream_outage=_STATE.get("upstream_outage", False),
        )
        _STATE["tick_count_ok"] += 1

    except Exception as exc:  # noqa: BLE001
        duration = time.perf_counter() - start
        _STATE.update(
            {
                "last_tick_at": now.isoformat(),
                "last_tick_ok": False,
                "last_tick_duration_s": round(duration, 2),
                "last_error": f"{type(exc).__name__}: {exc}",
            }
        )
        _STATE["tick_count_failed"] += 1
        log_event("scan.error",
                  level=logging.ERROR,
                  exc_type=type(exc).__name__,
                  exc_msg=str(exc),
                  duration_s=round(duration, 2))
    finally:
        with _STATE_LOCK:
            _STATE["scan_in_flight"] = False
            # Rolling p50/p95 over the last 50 durations.
            durs: list[float] = _STATE["tick_durations"]
            durs.append(round(duration, 2))
            if len(durs) > 50:
                del durs[: len(durs) - 50]
            if durs:
                srt = sorted(durs)
                p50_idx = len(srt) // 2
                p95_idx = max(0, int(len(srt) * 0.95) - 1)
                _STATE["tick_p50_duration_s"] = srt[p50_idx]
                _STATE["tick_p95_duration_s"] = srt[p95_idx]


@app.post("/api/tick", status_code=202)
def tick(
    background_tasks: BackgroundTasks,
    x_owl_secret: str | None = Header(default=None),
) -> dict[str, Any]:
    """Schedule a watchlist scan to run in the background.

    Returns 202 Accepted immediately so external cron callers (GitHub
    Actions) never see a timeout no matter how long the scan takes.
    Watch progress / result via ``/api/health`` (``scan_in_flight``,
    ``last_tick_at``, ``last_tick_stations``, ``last_tick_flagged``).

    Protected by ``X-OWL-Secret`` header (must match ``OWL_CRON_SECRET`` env).
    Intended caller: a GitHub Actions scheduled workflow, ``*/5 * * * *``.

    Skips queueing a duplicate if a scan is already in flight — the cron
    runs every 5 min, scans typically take 30-90 s, so overlap shouldn't
    happen, but this is defensive.
    """
    _check_secret(x_owl_secret)

    # Atomic check-then-enqueue under the state lock.
    with _STATE_LOCK:
        _STATE["tick_count_total"] += 1
        if _STATE.get("scan_in_flight"):
            _STATE["tick_count_skipped_overlap"] += 1
            log_event("tick.skipped", reason="scan already in flight")
            return {
                "ok": True,
                "queued": False,
                "reason": "scan already in flight",
                "started_at": _STATE.get("scan_started_at"),
            }
        # Claim the slot BEFORE scheduling the background task so a
        # second concurrent request sees scan_in_flight=True immediately.
        _STATE["scan_in_flight"] = True
        _STATE["scan_started_at"] = datetime.now(timezone.utc).isoformat()

    # --- Outage cooldown --------------------------------------------
    # When the last scan classified as an upstream outage (IEM 429'd us,
    # NCEI also down, etc.), don't hammer those endpoints every 5 min —
    # wait at least OWL_OUTAGE_COOLDOWN_MIN (default 20) before trying
    # again.  Spares their rate-limit budget AND our CPU/logs.
    if _STATE.get("upstream_outage"):
        cooldown_min = int(os.environ.get("OWL_OUTAGE_COOLDOWN_MIN", "20"))
        last_at_iso = _STATE.get("last_tick_at")
        if last_at_iso:
            try:
                last_at = datetime.fromisoformat(last_at_iso)
                elapsed = (datetime.now(timezone.utc) - last_at).total_seconds()
                if elapsed < cooldown_min * 60:
                    remaining = int(cooldown_min * 60 - elapsed)
                    _STATE["tick_count_skipped_overlap"] += 1
                    log_event("tick.cooldown",
                              remaining_s=remaining,
                              reason="upstream outage cooldown")
                    return {
                        "ok": True,
                        "queued": False,
                        "reason": "upstream outage cooldown",
                        "retry_after_s": remaining,
                    }
            except Exception:
                pass

    background_tasks.add_task(_run_scan)
    log_event("tick.queued")
    return {
        "ok": True,
        "queued": True,
        "queued_at": datetime.now(timezone.utc).isoformat(),
        "watch_at": "/api/health",
    }


# ---------------------------------------------------------------------------
# GET /api/sources
# ---------------------------------------------------------------------------
@app.get("/api/sources")
def sources() -> dict[str, Any]:
    """Return the data source registry — trust tier, URL, last-fetched, etc."""
    try:
        from asos_tools.sources import SOURCES  # type: ignore[attr-defined]
        return {"sources": [s.__dict__ if hasattr(s, "__dict__") else s
                            for s in SOURCES]}
    except Exception:  # noqa: BLE001
        # Don't leak internal file paths / module names.
        _log.exception("/api/sources failed")
        import uuid as _uuid
        err_id = _uuid.uuid4().hex[:8]
        return JSONResponse(status_code=500, content={
            "error": "internal error",
            "request_id": err_id,
        })


# ---------------------------------------------------------------------------
# GET /api/webcams/near
# ---------------------------------------------------------------------------
@app.get("/api/webcams/near")
def webcams_near(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_nm: float = Query(25.0, gt=0, le=500),
) -> dict[str, Any]:
    """FAA WeatherCams within ``radius_nm`` nautical miles of (lat, lon)."""
    try:
        from asos_tools.webcams import cameras_near  # type: ignore[attr-defined]
        cams = cameras_near(lat, lon, radius_nm=radius_nm)
        return {"count": len(cams), "cameras": cams}
    except Exception:  # noqa: BLE001
        _log.exception("/api/webcams/near failed")
        import uuid as _uuid
        err_id = _uuid.uuid4().hex[:8]
        return JSONResponse(status_code=500, content={
            "error": "internal error",
            "request_id": err_id,
        })
