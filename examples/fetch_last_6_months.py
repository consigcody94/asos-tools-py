"""Pull the last 6 months of 1-minute ASOS data for a single station.

This is both a real-world demo and an acceptance test: a single ~260k-row
request to IEM's 1-minute service, with timing and summary stats.

Run:
    python examples/fetch_last_6_months.py
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone

from asos_tools import fetch_1min


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", default="KORD",
                        help="ICAO-style station ID (default: KORD)")
    parser.add_argument("--months", type=int, default=6,
                        help="Number of months back from now (default: 6)")
    parser.add_argument("--save", default=None,
                        help="Optional path to write the DataFrame as Parquet")
    args = parser.parse_args()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30 * args.months)

    print(f"Fetching {args.station} from {start.isoformat()} to {end.isoformat()}")
    print(f"(~{args.months} months of 1-minute data)")

    t0 = time.perf_counter()
    df = fetch_1min(args.station, start, end)
    elapsed = time.perf_counter() - t0

    print(f"\nElapsed: {elapsed:.1f}s")
    print(f"Rows:    {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    if not df.empty:
        print(f"First:   {df['valid'].iloc[0]}")
        print(f"Last:    {df['valid'].iloc[-1]}")

        numeric = df.select_dtypes(include="number")
        if not numeric.empty:
            print("\nSummary stats:")
            print(numeric.describe().round(2).T[["count", "mean", "std", "min", "max"]])

        if "precip" in df.columns:
            precip_total = float(df["precip"].sum(skipna=True))
            print(f"\nTotal precip over window: {precip_total:.2f} in")

    if args.save:
        df.to_parquet(args.save)
        print(f"\nWrote {args.save}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
