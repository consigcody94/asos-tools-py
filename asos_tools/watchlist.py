"""Operational ASOS watchlist for controllers.

Given a set of stations and a recent time window, classify each station's
current operational state:

    MISSING      - at least one scheduled hourly METAR did not come through;
                   the most critical state (silent = worse than degraded)
    FLAGGED      - most recent METAR ended with $ (maintenance indicator)
    INTERMITTENT - mixed flags in the window, not cleanly recovered
    RECOVERED    - was flagged earlier, last two reports clean, back online
    CLEAN        - no flagged or missing METARs in the window
    NO DATA      - IEM returned nothing for this station (never reporting
                   in this window)

Intended for a live 4-hour scan across all AOMC-certified ASOS stations so
operations folks can see at a glance which sites need attention, which
have gone dark, and which have already come back clean.

ASOS routine METAR schedule: one report per hour, typically at HH:51Z.
``build_watchlist`` treats any completed hour-bucket within the window
that has zero METARs as a missing scheduled report.

Powered by :func:`asos_tools.fetch_metars`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence, Union

import pandas as pd

from asos_tools.metars import decode_reasons_short, fetch_metars

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
    "probable_reason",
    "total",
    "flagged",
    "missing",
    "missing_hours_utc",
    "flag_rate",
    "expected_hourly",
    "latest_time",
    "latest_flag_time",
    "minutes_since_last_report",
    "minutes_since_last_flag",
    "latest_metar",
]

#: Preferred ordering for the ``status`` column in displays.
STATUS_ORDER = ["MISSING", "FLAGGED", "INTERMITTENT", "RECOVERED", "CLEAN", "NO DATA"]


def _expected_hourly_buckets(
    start: datetime,
    end: datetime,
    *,
    grace_minutes: int = 15,
) -> list[datetime]:
    """Return the wall-clock hour boundaries (UTC) inside ``[start, end]``
    whose scheduled :51 METAR should already have been filed.

    We skip the current in-progress hour unless ``end`` is at least
    ``grace_minutes`` past its :51 mark.
    """
    now = datetime.now(timezone.utc)
    # Cap at now - grace so an in-progress hour whose METAR hasn't been
    # filed yet isn't mislabelled as "missing".
    effective_end = min(end, now - timedelta(minutes=grace_minutes))
    if effective_end <= start:
        return []

    # First full hour >= start (round up).
    first_hour = start.replace(minute=0, second=0, microsecond=0)
    if first_hour < start:
        first_hour = first_hour + timedelta(hours=1)

    buckets: list[datetime] = []
    current = first_hour
    while current + timedelta(hours=1) <= effective_end + timedelta(minutes=grace_minutes):
        buckets.append(current)
        current += timedelta(hours=1)
    return buckets


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

    # Compute expected hour buckets ONCE — shared across all stations.
    expected_buckets = _expected_hourly_buckets(start, end)
    expected_count = len(expected_buckets)
    expected_set = set(b.replace(tzinfo=timezone.utc) for b in expected_buckets)

    metars = fetch_metars(station_list, start, end)

    rows: list[dict] = []
    seen_ids: set[str] = set()

    def _latest_expected_bucket() -> datetime | None:
        return expected_buckets[-1] if expected_buckets else None

    if not metars.empty:
        for raw_stn, sub in metars.groupby("station"):
            sub = sub.sort_values("valid")
            stn = str(raw_stn)
            seen_ids.add(stn)
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

            # --- Missing METAR detection ---
            # For each expected hour bucket, check whether any METAR falls
            # inside the window [bucket, bucket+1h]. A bucket with zero
            # reports in that range is a missing scheduled METAR.
            covered_buckets: set[datetime] = set()
            for ts in sub["valid"]:
                py_ts = ts.to_pydatetime()
                # Round DOWN to the hour this METAR falls in.
                bucket = py_ts.replace(minute=0, second=0, microsecond=0)
                if bucket in expected_set:
                    covered_buckets.add(bucket)
                # Reports filed early (e.g. SPECI at HH:47 for the HH bucket)
                # should also count toward the prior hour if close to top.
                if py_ts.minute >= 45:
                    adj = (py_ts + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                    if adj in expected_set:
                        covered_buckets.add(adj)
            missing_buckets = sorted(b for b in expected_buckets
                                     if b not in covered_buckets)
            missing_count = len(missing_buckets)
            missing_hours_label = ", ".join(b.strftime("%H:%MZ")
                                            for b in missing_buckets[:4])
            if missing_count > 4:
                missing_hours_label += f", +{missing_count - 4} more"

            # Is the *most recent* expected hour missing? That's a signal
            # the station has gone silent right now.
            latest_bucket = _latest_expected_bucket()
            latest_bucket_missing = (
                latest_bucket is not None and latest_bucket not in covered_buckets
            )

            # --- Classification (priority order) ---
            if latest_bucket_missing or missing_count > 0:
                status = "MISSING"
            elif latest_is_flagged:
                status = "FLAGGED"
            elif flagged == 0:
                status = "CLEAN"
            else:
                tail = sub.tail(2)
                if (~tail["has_maintenance"]).all() and len(tail) >= 2:
                    status = "RECOVERED"
                else:
                    status = "INTERMITTENT"

            mins_since_flag: float | None = None
            if pd.notna(latest_flag_time):
                delta = end - latest_flag_time.to_pydatetime()
                mins_since_flag = round(delta.total_seconds() / 60.0, 1)

            mins_since_report: float | None = None
            if pd.notna(latest_time):
                delta = end - latest_time.to_pydatetime()
                mins_since_report = round(delta.total_seconds() / 60.0, 1)

            lookup_id = "K" + stn if ("K" + stn) in meta_map else stn
            meta = meta_map.get(lookup_id, {})

            # Decode probable reason from the latest METAR (or latest flagged one).
            if flagged > 0:
                last_flagged_metar = sub.loc[sub["has_maintenance"], "metar"].iloc[-1]
                reason = decode_reasons_short(last_flagged_metar)
            elif missing_count > 0:
                reason = "No METAR received"
            else:
                reason = ""

            rows.append({
                "station": lookup_id,
                "name": meta.get("name", "") or "",
                "state": meta.get("state", "") or "",
                "status": status,
                "probable_reason": reason,
                "total": total,
                "flagged": flagged,
                "missing": missing_count,
                "missing_hours_utc": missing_hours_label,
                "flag_rate": round(flagged / total * 100, 1) if total else 0.0,
                "expected_hourly": expected_count,
                "latest_time": latest_time,
                "latest_flag_time": latest_flag_time,
                "minutes_since_last_report": mins_since_report,
                "minutes_since_last_flag": mins_since_flag,
                "latest_metar": latest_metar,
            })

    # Stations with no METARs at all in the window.
    for sid in station_list:
        candidates = {sid, "K" + sid, sid[1:] if sid.startswith("K") else sid}
        if not (candidates & seen_ids):
            meta = meta_map.get(sid, {})
            # A station that produced zero METARs: every expected bucket is
            # missing. If expected_count > 0 we call it MISSING, otherwise
            # NO DATA (e.g. scan window too short for any full hour).
            if expected_count > 0:
                missing_label = ", ".join(b.strftime("%H:%MZ")
                                          for b in expected_buckets[:4])
                if expected_count > 4:
                    missing_label += f", +{expected_count - 4} more"
                status = "MISSING"
            else:
                missing_label = ""
                status = "NO DATA"

            rows.append({
                "station": sid,
                "name": meta.get("name", "") or "",
                "state": meta.get("state", "") or "",
                "status": status,
                "probable_reason": "No METAR received" if status == "MISSING" else "",
                "total": 0,
                "flagged": 0,
                "missing": expected_count if status == "MISSING" else 0,
                "missing_hours_utc": missing_label,
                "flag_rate": 0.0,
                "expected_hourly": expected_count,
                "latest_time": pd.NaT,
                "latest_flag_time": pd.NaT,
                "minutes_since_last_report": None,
                "minutes_since_last_flag": None,
                "latest_metar": "",
            })

    if not rows:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)

    df = pd.DataFrame(rows)
    # Sort so MISSING first, then FLAGGED, INTERMITTENT, RECOVERED, CLEAN, NO DATA.
    # Within each status, worst-first: more missing or higher flag_rate first.
    status_rank = {s: i for i, s in enumerate(STATUS_ORDER)}
    df["_rank"] = df["status"].map(status_rank).fillna(99)
    df = df.sort_values(
        ["_rank", "missing", "flag_rate", "station"],
        ascending=[True, False, False, True],
    ).reset_index(drop=True)
    df = df.drop(columns="_rank")
    return df[WATCHLIST_COLUMNS]
