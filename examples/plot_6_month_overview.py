"""Generate a 3-panel overview plot from 6 months of 1-minute data."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from asos_tools import fetch_1min


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KJFK")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--out", default="images/six_month_overview.png")
    args = parser.parse_args()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30 * args.months)

    print(f"Fetching {args.station} {start.date()} -> {end.date()} ...")
    df = fetch_1min(args.station, start, end)
    name = df["station_name"].iloc[0] if not df.empty else args.station
    print(f"{len(df):,} rows ({name})")

    fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True)

    # Temperature + dewpoint
    axes[0].plot(df["valid"], df["tmpf"], color="#d62728", linewidth=0.4, label="Temp")
    axes[0].plot(df["valid"], df["dwpf"], color="#1f77b4", linewidth=0.4, label="Dewpoint")
    axes[0].set_ylabel("°F")
    axes[0].legend(loc="upper right", framealpha=0.9)
    axes[0].grid(True, alpha=0.3)

    # Wind speed + gusts
    axes[1].plot(df["valid"], df["sknt"], color="#2ca02c", linewidth=0.3, label="Wind")
    axes[1].plot(df["valid"], df["gust_sknt"], color="#9467bd", linewidth=0.3, label="Gust")
    axes[1].set_ylabel("knots")
    axes[1].legend(loc="upper right", framealpha=0.9)
    axes[1].grid(True, alpha=0.3)

    # Precipitation (resampled to hourly for readability)
    precip_hourly = (df.set_index("valid")["precip"]
                       .resample("1h")
                       .sum(min_count=1))
    axes[2].bar(precip_hourly.index, precip_hourly.values,
                width=1/24, color="#17becf", align="edge")
    axes[2].set_ylabel("precip (in/hr)")
    axes[2].grid(True, alpha=0.3)

    axes[2].xaxis.set_major_locator(mdates.MonthLocator())
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.suptitle(f"{name} — {args.months}-month 1-minute overview "
                 f"({len(df):,} observations)")
    fig.tight_layout()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120, bbox_inches="tight")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
