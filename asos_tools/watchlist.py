"""Operational ASOS `$` watchlist for controllers.

Given a set of stations and a recent time window, classify each station's
current maintenance-flag state:

    FLAGGED      - most recent METAR ended with $
    RECOVERED    - was flagged earlier in the window, latest report is clean
    INTERMITTENT - mixed flags in the window, not cleanly recovered
    CLEAN        - no flagged METARs in the window
    NO DATA      - IEM returned nothing for this station

The intended use is a live 4-hour scan across all AOMC-certified ASOS
stations so operations folks can see at a glance which sites need
attention and which have already come back clean.

Powered by :func:`asos_tools.fetch_metars`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence, Union

import pandas as pd

from asos_tools.metars import fetch_metars

__all__ = [
    "build_watchlist",
    "WATCHLIST_COLUMNS",
    "STATUS_ORDER",
]

#: Column order in the returned DataFrame.
WATCHLIST_COLUMNS = [
    "station",
    "name",
    "state",
    "status",
    "total",
    "flagged",
    "flag_rate",
    "latest_time",
    "latest_flag_time",
    "minutes_since_last_flag",
    "latest_metar",
]

#: Preferred ordering for the ``status`` column in displays.
STATUS_ORDER = ["FLAGGED", "INTERMITTENT", "RECOVERED", "CLEAN", "NO DATA"]


def build_watchlist(
    stations: Union[Sequence[str], Iterable[dict]],
    *,
    hours: float = 4.0,
    end: datetime | None = None,
    station_metadata: Iterable[dict] | None = None,
) -> pd.DataFrame:
    """Return a per-station operational status summary.

    Parameters
    ----------
    stations
        Either an iterable of station id strings, or an iterable of
        station dicts (id, name, state, ...).
    hours
        How far back to scan (default 4 hours).
    end
        End of the scan window; defaults to ``datetime.now(UTC)``.
    station_metadata
        Optional iterable of station dicts (with keys ``id``, ``name``,
        ``state``). If provided, used to enrich the output with station
        names and states. If ``stations`` is already an iterable of dicts,
        that is used automatically.

    Returns
    -------
    pandas.DataFrame with columns defined in :data:`WATCHLIST_COLUMNS`,
    sorted so currently-flagged stations come first.
    """
    end = end or datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)

    # Normalize input into (id_list, meta_map).
    if not stations:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)

    first = next(iter(stations))
    if isinstance(first, dict):
        station_list = [s["id"] for s in stations if s.get("id")]
        meta_map = {s["id"]: s for s in stations if s.get("id")}
    else:
        station_list = [s for s in stations if s]
        meta_map = {}
        if station_metadata:
            for m in station_metadata:
                if m.get("id"):
                    meta_map[m["id"]] = m

    if not station_list:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)

    metars = fetch_metars(station_list, start, end)

    rows: list[dict] = []
    seen_ids: set[str] = set()

    if not metars.empty:
        for raw_stn, sub in metars.groupby("station"):
            sub = sub.sort_values("valid")
            stn = str(raw_stn)
            seen_ids.add(stn)
            # Also add K-prefixed variant so we can match meta_map entries
            # that use canonical ICAO (KJFK) rather than IEM's bare id (JFK).
            seen_ids.add("K" + stn)

            total = len(sub)
            flagged = int(sub["has_maintenance"].sum())
            latest_row = sub.iloc[-1]
            latest_time: pd.Timestamp = latest_row["valid"]
            latest_metar = latest_row["metar"]
            latest_is_flagged = bool(latest_row["has_maintenance"])

            if flagged > 0:
                latest_flag_time = sub.loc[sub["has_maintenance"], "valid"].max()
            else:
                latest_flag_time = pd.NaT

            # Classification.
            if latest_is_flagged:
                status = "FLAGGED"
            elif flagged == 0:
                status = "CLEAN"
            else:
                # Have flags but latest is clean. If the tail is solidly
                # clean (last 2 reports clean), call it RECOVERED.
                tail = sub.tail(2)
                if (~tail["has_maintenance"]).all() and len(tail) >= 2:
                    status = "RECOVERED"
                else:
                    status = "INTERMITTENT"

            mins_since_flag: float | None = None
            if pd.notna(latest_flag_time):
                delta = end - latest_flag_time.to_pydatetime()
                mins_since_flag = round(delta.total_seconds() / 60.0, 1)

            # Map back to the canonical ICAO id for display/lookup.
            lookup_id = "K" + stn if ("K" + stn) in meta_map else stn
            meta = meta_map.get(lookup_id, {})

            rows.append({
                "station": lookup_id,
                "name": meta.get("name", "") or "",
                "state": meta.get("state", "") or "",
                "status": status,
                "total": total,
                "flagged": flagged,
                "flag_rate": round(flagged / total * 100, 1) if total else 0.0,
                "latest_time": latest_time,
                "latest_flag_time": latest_flag_time,
                "minutes_since_last_flag": mins_since_flag,
                "latest_metar": latest_metar,
            })

    # Add "NO DATA" rows for stations we asked about but got nothing back.
    for sid in station_list:
        candidates = {sid, "K" + sid, sid[1:] if sid.startswith("K") else sid}
        if not (candidates & seen_ids):
            meta = meta_map.get(sid, {})
            rows.append({
                "station": sid,
                "name": meta.get("name", "") or "",
                "state": meta.get("state", "") or "",
                "status": "NO DATA",
                "total": 0,
                "flagged": 0,
                "flag_rate": 0.0,
                "latest_time": pd.NaT,
                "latest_flag_time": pd.NaT,
                "minutes_since_last_flag": None,
                "latest_metar": "",
            })

    if not rows:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)

    df = pd.DataFrame(rows)
    # Sort so FLAGGED first, then INTERMITTENT, RECOVERED, CLEAN, NO DATA.
    status_rank = {s: i for i, s in enumerate(STATUS_ORDER)}
    df["_rank"] = df["status"].map(status_rank).fillna(99)
    df = df.sort_values(["_rank", "flag_rate", "station"],
                        ascending=[True, False, True]).reset_index(drop=True)
    df = df.drop(columns="_rank")
    return df[WATCHLIST_COLUMNS]
