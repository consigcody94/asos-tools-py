"""Structured JSON logging for O.W.L.

On Hugging Face Spaces the only observability surface is the container's
stdout/stderr stream (viewable at ``/logs/container``). Plain-text logs
are hard to grep; JSON-per-line makes it trivial to pipe through ``jq``
or tail into a downstream log aggregator.

Usage
-----
Call :func:`install_json_logging` once at process startup (we do this in
``api.py``'s module import). After that, every ``logging`` call anywhere
in the app emits a single-line JSON record:

    {"ts":"2026-04-19T06:45:12.345Z","level":"INFO","logger":"owl.api",
     "event":"scan.ok","stations":920,"flagged":42,"duration_s":45.6}

For structured fields, pass them via ``logger.info("msg", extra={...})``
or use the shortcut :func:`log_event` below::

    from asos_tools.logging_ext import log_event
    log_event("scan.ok", stations=920, flagged=42, duration_s=45.6)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format each record as a one-line JSON object.

    Standard fields: ``ts, level, logger, msg``.
    Any ``extra={}`` keys are merged in at the top level.
    Exceptions are flattened into ``exc_type, exc_msg``.
    """

    _RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "asctime", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
                  .isoformat(timespec="milliseconds")
                  .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Merge any extras the caller passed via ``extra={}``.
        for k, v in record.__dict__.items():
            if k not in self._RESERVED and not k.startswith("_"):
                try:
                    json.dumps(v)   # check serializability
                    payload[k] = v
                except Exception:
                    payload[k] = repr(v)
        if record.exc_info:
            exc_type, exc, _ = record.exc_info
            payload["exc_type"] = exc_type.__name__ if exc_type else "Unknown"
            payload["exc_msg"] = str(exc) if exc else ""
        try:
            return json.dumps(payload, separators=(",", ":"), default=str)
        except Exception:
            # Fallback: stringify everything.
            return json.dumps({k: str(v) for k, v in payload.items()},
                              separators=(",", ":"))


def install_json_logging(level: str | int | None = None) -> None:
    """Replace root logger handlers with a single stdout JSON handler.

    Idempotent — calling more than once is safe. Honors ``OWL_LOG_LEVEL``
    env var if ``level`` isn't passed.
    """
    root = logging.getLogger()
    # Drop any existing handlers (Streamlit adds a StreamHandler on import).
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)

    effective = level or os.environ.get("OWL_LOG_LEVEL", "INFO")
    if isinstance(effective, str):
        effective = getattr(logging, effective.upper(), logging.INFO)
    root.setLevel(effective)


def log_event(event: str, level: int = logging.INFO, **fields: Any) -> None:
    """Emit a structured event.

    ``event`` is a dot-separated tag (``scan.ok``, ``fetch.retry``,
    ``alert.sent``). ``**fields`` become top-level JSON keys.
    """
    logging.getLogger("owl.event").log(
        level, event, extra={"event": event, **fields}
    )
