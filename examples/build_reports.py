"""Generate a family of dashboard reports.

Three report families are built per run:

1.  **1-minute station report** — one per time window (1 / 7 / 14 / 30 day).
2.  **Maintenance-flag report** — one METAR dashboard per window showing
    which stations are flagging themselves with the ``$`` maintenance
    indicator.
3.  **Comparison report** — ``$``-flagged vs clean METARs, per window.

Examples
--------
::

    # Single station, KJFK
    python examples/build_reports.py --station KJFK

    # Preset group (from asos_tools.stations)
    python examples/build_reports.py --group long_island

    # Custom list
    python examples/build_reports.py --sites KJFK KLGA KEWR
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from asos_tools import fetch_1min, fetch_metars
from asos_tools.report import (
    build_comparison_report,
    build_maintenance_report,
    build_report,
)
from asos_tools.stations import GROUPS, get_group, list_groups


WINDOWS: list[tuple[str, int, str]] = [
    ("1 day",  1,  "1day"),
    ("7 day",  7,  "7day"),
    ("14 day", 14, "14day"),
    ("30 day", 30, "30day"),
]


def _resolve_stations(args) -> tuple[list[str], str]:
    """Figure out which station list and a human-friendly label to use."""
    if args.group:
        stations = list(get_group(args.group))
        label = args.group.replace("_", " ").title()
    elif args.sites:
        stations = args.sites
        label = " · ".join(stations)
    else:
        stations = [args.station]
        label = args.station
    return stations, label


def _slug(text: str) -> str:
    return (text.lower()
                .replace("/", "-").replace(" ", "-")
                .replace("·", "").replace("--", "-").strip("-"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--station", default="KJFK",
                     help="Single station (default: KJFK)")
    src.add_argument("--sites", nargs="+",
                     help="Explicit list of ICAO station IDs")
    src.add_argument("--group", choices=list_groups(),
                     help="Preset station group from asos_tools.stations")
    parser.add_argument("--anchor", default=None,
                        help="UTC anchor datetime for 'now' as ISO 8601 "
                             "(default: current UTC time).")
    parser.add_argument("--outdir", default="images/reports")
    parser.add_argument("--skip-1min", action="store_true",
                        help="Skip the per-station 1-minute reports")
    parser.add_argument("--skip-metar", action="store_true",
                        help="Skip the maintenance + comparison METAR reports")
    args = parser.parse_args()

    stations, group_label = _resolve_stations(args)
    anchor = (datetime.fromisoformat(args.anchor).replace(tzinfo=timezone.utc)
              if args.anchor else datetime.now(timezone.utc))
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Stations: {stations}")
    print(f"Group label: {group_label}")
    print(f"Anchor: {anchor.isoformat()}\n")

    # --- 1-minute reports (per-station, per-window) ---
    if not args.skip_1min:
        for label, days, slug in WINDOWS:
            start = anchor - timedelta(days=days)
            for station in stations:
                print(f"[1-min] {station} {label:<6} ({start:%Y-%m-%d} -> {anchor:%Y-%m-%d})")
                try:
                    df = fetch_1min(station, start, anchor)
                except Exception as e:
                    print(f"   fetch error: {e}")
                    continue
                if df.empty:
                    print("   no data, skipped")
                    continue
                out_path = out_dir / f"{station.lower()}_{slug}.png"
                build_report(
                    df,
                    window_label=label,
                    station_id=station,
                    station_name=str(df["station_name"].iloc[0]),
                    out_path=out_path,
                )
                print(f"   {len(df):>7,} rows  ->  {out_path}")

    # --- METAR-driven reports (group-level) ---
    if not args.skip_metar:
        for label, days, slug in WINDOWS:
            start = anchor - timedelta(days=days)
            print(f"\n[METAR] group={group_label!r}  window={label}")
            try:
                metars = fetch_metars(stations, start, anchor)
            except Exception as e:
                print(f"   fetch error: {e}")
                continue
            if metars.empty:
                print("   no METARs, skipped")
                continue
            print(f"   {len(metars):,} METARs, "
                  f"{metars['has_maintenance'].sum():,} flagged "
                  f"({100*metars['has_maintenance'].mean():.1f}%)")

            group_slug = _slug(group_label)
            maint_path = out_dir / f"{group_slug}_{slug}_maintenance.png"
            build_maintenance_report(
                metars,
                group_label=group_label,
                window_label=label,
                out_path=maint_path,
            )
            print(f"   ->  {maint_path}")

            cmp_path = out_dir / f"{group_slug}_{slug}_comparison.png"
            build_comparison_report(
                metars,
                group_label=group_label,
                window_label=label,
                out_path=cmp_path,
            )
            print(f"   ->  {cmp_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
