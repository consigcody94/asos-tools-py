"""One-shot fix for mis-prefixed ICAO IDs in asos_tools/data/aomc_stations.json.

Original bug: the catalog builder (deploy/build_aomc_catalog.py) blindly
prepended "K" to every 3-letter FAA LID, regardless of state. That produces
wrong IDs like ``KANC`` for Anchorage (actual ICAO: ``PANC``), ``KHNL`` for
Honolulu (``PHNL``), ``KTJSJ``-style mistakes for Puerto Rico, etc.

This script walks the bundled JSON, replaces every non-CONUS station's
"id" with the correct ICAO prefix based on its state, leaves "call" alone
(the 3-letter FAA LID is still accurate), writes back the JSON in place.

ICAO prefix map (official, per ICAO Doc 7910):

    AK  -> PA    (Pacific - Alaska)
    HI  -> PH    (Pacific - Hawaii)
    GU  -> PG    (Guam)
    MP  -> PG    (Northern Mariana Islands - also P-region)
    AS  -> NS    (American Samoa)
    PR  -> TJ    (San Juan FIR)
    VI  -> TI    (US Virgin Islands)
    (CONUS + other US = K prefix, already correct)

Run idempotent — safe to re-run.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


_PREFIX_BY_STATE = {
    "AK": "PA",
    "HI": "PH",
    "GU": "PG",
    "MP": "PG",
    "AS": "NS",
    "PR": "TJ",
    "VI": "TI",
}


def _new_id(old_id: str, call: str, state: str) -> str:
    """Return the correct ICAO for a (call, state) pair, or old_id if no change."""
    state = (state or "").upper().strip()
    call = (call or "").upper().strip()
    if state not in _PREFIX_BY_STATE:
        return old_id  # CONUS + rare — leave alone

    # If old_id already has the correct prefix, keep it.
    correct_prefix = _PREFIX_BY_STATE[state]
    if old_id.startswith(correct_prefix):
        return old_id

    # Strip a wrong K prefix if present, else just use call.
    base = call if call else old_id
    if old_id.startswith("K") and len(old_id) == 4:
        # Confirm: old_id[1:] should equal call (the 3-letter FAA LID).
        base = old_id[1:] if len(old_id) == 4 else call

    # Handle 3-letter FAA LIDs like "TUT" → "NSTU" for American Samoa.
    if state == "AS" and base == "TUT":
        return "NSTU"
    # American Samoa generally uses 4-char IDs starting with NS.
    if state == "AS":
        return "NS" + base[-2:] if len(base) == 3 else base

    return correct_prefix + base


def main() -> int:
    p = Path(__file__).resolve().parent / "asos_tools" / "data" / "aomc_stations.json"
    if not p.exists():
        print(f"ERROR: {p} not found", file=sys.stderr)
        return 2

    data = json.loads(p.read_text(encoding="utf-8"))
    stations = data.get("stations", [])
    print(f"Loaded {len(stations)} stations from {p.name}")

    # Collect changes before applying, for the audit log.
    changes: list[tuple[str, str, str, str]] = []
    for rec in stations:
        old = rec.get("id", "")
        call = rec.get("call", "")
        state = rec.get("state", "")
        new = _new_id(old, call, state)
        if new != old:
            changes.append((state, old, new, rec.get("name", "")))
            rec["id"] = new

    print(f"\nFixed prefixes on {len(changes)} stations:")
    # Group by state for readability.
    by_state: dict[str, list] = {}
    for st, old, new, name in changes:
        by_state.setdefault(st, []).append((old, new, name))
    for st in sorted(by_state):
        rows = by_state[st]
        print(f"\n  {st} ({len(rows)} stations):")
        for old, new, name in rows[:4]:
            print(f"    {old:6s} -> {new:6s}  {name[:45]}")
        if len(rows) > 4:
            print(f"    ... and {len(rows) - 4} more")

    # Overwrite source JSON.
    data["stations"] = stations
    data["prefix_fix_applied_utc"] = __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    ).isoformat(timespec="seconds")
    p.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote corrected {p.name} ({p.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
