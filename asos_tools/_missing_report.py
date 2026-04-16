"""Missing-METAR report builder — appended to the report module at import time."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

from asos_tools.report import (
    ACCENT, BG_CHIP, BG_OUTER, BORDER, FG, FG_HI, MUTED, SOFT,
    ReportResult, _apply_style, _auto_time_axis, _draw_chip_strip,
)

C_MISS = "#ef4444"
C_PRESENT = "#34d399"


def _compute_gap_table(metars_df, expected_buckets):
    expected_set = set(expected_buckets)
    rows = []
    for stn, sub in metars_df.groupby("station"):
        sub = sub.sort_values("valid")
        covered = set()
        for ts in sub["valid"]:
            py_ts = ts.to_pydatetime()
            bucket = py_ts.replace(minute=0, second=0, microsecond=0)
            if bucket in expected_set:
                covered.add(bucket)
            if py_ts.minute >= 45:
                adj = (py_ts + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0)
                if adj in expected_set:
                    covered.add(adj)
        missed = sorted(b for b in expected_buckets if b not in covered)
        longest_gap_h = 0
        if missed:
            run = 1
            for a, b in zip(missed[:-1], missed[1:]):
                if (b - a).total_seconds() == 3600:
                    run += 1
                else:
                    longest_gap_h = max(longest_gap_h, run)
                    run = 1
            longest_gap_h = max(longest_gap_h, run)
        rows.append({
            "station": str(stn),
            "reports": len(sub),
            "expected": len(expected_buckets),
            "covered": len(covered),
            "missing": len(missed),
            "coverage_pct": round(
                len(covered) / max(len(expected_buckets), 1) * 100, 1),
            "longest_gap_h": longest_gap_h,
            "missed_hours": ", ".join(
                b.strftime("%H:%MZ") for b in missed[:6]
            ) + (f" +{len(missed)-6}" if len(missed) > 6 else ""),
        })
    return (pd.DataFrame(rows)
            .sort_values("missing", ascending=False)
            .reset_index(drop=True))


def build_missing_report(
    metars_df: pd.DataFrame,
    *,
    group_label: str,
    window_label: str,
    expected_buckets: list | None = None,
    out_path: Union[str, Path],
) -> ReportResult:
    """Render a missing-METAR breakdown showing when stations went silent,
    gap durations, coverage heatmap, and hourly METAR volume."""
    if metars_df.empty:
        raise ValueError("Cannot build missing report from empty DataFrame")
    _apply_style()

    if expected_buckets is None:
        from asos_tools.watchlist import _expected_hourly_buckets
        t_min = metars_df["valid"].min().to_pydatetime()
        t_max = metars_df["valid"].max().to_pydatetime()
        expected_buckets = _expected_hourly_buckets(t_min, t_max)

    n_expected = len(expected_buckets)
    gap_df = _compute_gap_table(metars_df, expected_buckets)
    with_gaps = gap_df[gap_df["missing"] > 0]
    full_cov = gap_df[gap_df["missing"] == 0]
    n_stn = metars_df["station"].nunique()

    fig = plt.figure(figsize=(14, 10), dpi=120)
    gs = GridSpec(4, 2, height_ratios=[1.15, 2.0, 1.5, 1.3],
                  hspace=0.55, wspace=0.22,
                  left=0.055, right=0.97, top=0.96, bottom=0.06)

    # ---- Header ----
    ax_h = fig.add_subplot(gs[0, :])
    ax_h.set_axis_off()
    ax_h.add_patch(Rectangle(
        (0.0, 0.96), 1.0, 0.04,
        transform=ax_h.transAxes, color=ACCENT, clip_on=False, zorder=10))
    ax_h.text(0.005, 0.98, "O.W.L.  —  OBSERVATION WATCH LOG",
              fontsize=7, color="#ffffff", fontweight="bold",
              transform=ax_h.transAxes, va="center", zorder=11)
    ax_h.text(0.99, 0.98, "MISSING METAR REPORT",
              fontsize=7, color="#ffffff",
              transform=ax_h.transAxes, va="center", ha="right", zorder=11)
    nfs = 22 if len(group_label) <= 18 else 15
    ax_h.text(0.0, 0.74, group_label, fontsize=nfs, fontweight="bold",
              color=FG_HI, transform=ax_h.transAxes, va="center")
    ax_h.text(0.0, 0.55,
              f"{window_label.upper()}  |  {n_stn} STATIONS  |  "
              f"{len(metars_df):,} METARS  |  {n_expected} EXPECTED SLOTS",
              fontsize=9.2, color=SOFT, transform=ax_h.transAxes, va="center")
    _draw_chip_strip(ax_h, [
        ("STATIONS W/ GAPS", f"{len(with_gaps):,}", C_MISS),
        ("FULL COVERAGE", f"{len(full_cov):,}", C_PRESENT),
        ("TOTAL MISSING HRS", f"{int(gap_df['missing'].sum()):,}", C_MISS),
        ("AVG COVERAGE", f"{gap_df['coverage_pct'].mean():.0f}%", ACCENT),
    ])

    # ---- Coverage bars (stations with gaps, top 20) ----
    ax_c = fig.add_subplot(gs[1, 0])
    ax_c.set_title("COVERAGE BY STATION (gaps only)", loc="left")
    if not with_gaps.empty:
        top = with_gaps.head(20).sort_values("coverage_pct")
        colors = [
            C_MISS if c < 50 else ("#fbbf24" if c < 80 else C_PRESENT)
            for c in top["coverage_pct"]
        ]
        bars = ax_c.barh(top["station"], top["coverage_pct"],
                         color=colors, edgecolor="none", height=0.72)
        ax_c.set_xlim(0, 105)
        ax_c.set_xlabel("% of expected hours covered")
        ax_c.grid(True, axis="x", alpha=0.4)
        for bar, pct, ms in zip(bars, top["coverage_pct"], top["missing"]):
            ax_c.text(pct + 1, bar.get_y() + bar.get_height() / 2,
                      f"{pct:.0f}% ({ms} missed)", va="center",
                      color=SOFT, fontsize=7.5)
    else:
        ax_c.text(0.5, 0.5, "All stations have full coverage!",
                  transform=ax_c.transAxes, ha="center", va="center",
                  color=C_PRESENT, fontsize=11)
        ax_c.set_axis_off()

    # ---- Heatmap (station x hour bucket) ----
    ax_ht = fig.add_subplot(gs[1, 1])
    ax_ht.set_title("COVERAGE HEATMAP", loc="left")
    if not with_gaps.empty and expected_buckets:
        from matplotlib.colors import ListedColormap
        from matplotlib.patches import Patch
        stn_order = list(
            with_gaps.head(20)
            .sort_values("missing", ascending=False)["station"]
        )
        matrix = np.zeros((len(stn_order), len(expected_buckets)))
        es = set(expected_buckets)
        for i, stn in enumerate(stn_order):
            sb = metars_df[metars_df["station"] == stn].sort_values("valid")
            cv = set()
            for ts in sb["valid"]:
                pt = ts.to_pydatetime()
                bk = pt.replace(minute=0, second=0, microsecond=0)
                if bk in es:
                    cv.add(bk)
                if pt.minute >= 45:
                    ad = (pt + timedelta(hours=1)).replace(
                        minute=0, second=0, microsecond=0)
                    if ad in es:
                        cv.add(ad)
            for j, b in enumerate(expected_buckets):
                matrix[i, j] = 1.0 if b in cv else 0.0
        cmap = ListedColormap([C_MISS, C_PRESENT])
        ax_ht.imshow(matrix, aspect="auto", cmap=cmap,
                     vmin=0, vmax=1, interpolation="nearest")
        ax_ht.set_yticks(range(len(stn_order)))
        ax_ht.set_yticklabels(stn_order, fontsize=7, color=FG)
        nb = len(expected_buckets)
        tk = np.linspace(0, nb - 1, min(8, nb)).astype(int)
        ax_ht.set_xticks(tk)
        ax_ht.set_xticklabels(
            [expected_buckets[i].strftime("%H:%MZ") for i in tk],
            fontsize=7, color=MUTED)
        ax_ht.legend(
            [Patch(color=C_PRESENT), Patch(color=C_MISS)],
            ["reported", "missing"],
            loc="lower right", fontsize=7.5, framealpha=0.8)
    else:
        ax_ht.text(0.5, 0.5, "No gaps to show",
                   transform=ax_ht.transAxes, ha="center", va="center",
                   color=MUTED)
        ax_ht.set_axis_off()

    # ---- Timeline: METARs received per hour ----
    ax_t = fig.add_subplot(gs[2, :])
    ax_t.set_title("METARS RECEIVED PER HOUR (all stations)", loc="left")
    if expected_buckets:
        hourly = (metars_df
                  .assign(bucket=metars_df["valid"].dt.floor("1h"))
                  .groupby("bucket").size())
        for b in expected_buckets:
            ts = pd.Timestamp(b).tz_localize("UTC") if b.tzinfo is None else pd.Timestamp(b)
            if ts not in hourly.index:
                hourly[ts] = 0
        hourly = hourly.sort_index()
        cb = [C_PRESENT if v > 0 else C_MISS for v in hourly.values]
        ax_t.bar(hourly.index, hourly.values, width=1.0 / 24,
                 color=cb, edgecolor="none", alpha=0.9, align="edge")
        ax_t.set_ylabel("METARs")
        ax_t.grid(True, axis="y", alpha=0.5)
        ax_t.margins(x=0.005)
        _auto_time_axis(ax_t, metars_df["valid"])
        ax_t.axhline(n_stn, color=ACCENT, lw=0.8,
                     ls=(0, (4, 3)), alpha=0.7)
        ax_t.text(0.01, n_stn, f"expected ~{n_stn}",
                  transform=ax_t.get_yaxis_transform(),
                  color=ACCENT, fontsize=7.5, va="bottom")
    else:
        ax_t.set_axis_off()

    # ---- Longest consecutive gaps ----
    ax_g = fig.add_subplot(gs[3, :])
    ax_g.set_axis_off()
    ax_g.set_title("LONGEST CONSECUTIVE GAPS", loc="left")
    worst = with_gaps.nlargest(6, "longest_gap_h")
    if worst.empty:
        ax_g.text(0.5, 0.5, "No gaps detected.",
                  transform=ax_g.transAxes, ha="center", va="center",
                  color=C_PRESENT, fontsize=11)
    else:
        ax_g.add_patch(Rectangle(
            (0.0, 0.02), 1.0, 0.82, transform=ax_g.transAxes,
            facecolor=BG_CHIP, edgecolor=BORDER, linewidth=0.8))
        for i, (_, row) in enumerate(worst.iterrows()):
            y = 0.75 - i * 0.12
            ax_g.text(0.012, y, row["station"],
                      transform=ax_g.transAxes, fontsize=9,
                      color=FG_HI, fontweight="bold", family="monospace")
            ax_g.text(0.08, y,
                      f"{row['longest_gap_h']}h gap  |  "
                      f"{row['missing']}/{row['expected']} missed  |  "
                      f"coverage {row['coverage_pct']:.0f}%  |  "
                      f"missed: {row['missed_hours']}",
                      transform=ax_g.transAxes, fontsize=8.5,
                      color=SOFT, family="monospace")

    # ---- Footer ----
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    fig.text(0.055, 0.018,
             "DATA  NOAA/NCEI ASOS METAR archive  via  IEM",
             fontsize=7.2, color=MUTED, ha="left")
    fig.text(0.97, 0.018,
             f"Generated {now}  |  O.W.L.",
             fontsize=7.2, color=MUTED, ha="right")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120, bbox_inches="tight", facecolor=BG_OUTER)
    plt.close(fig)
    return ReportResult(path=out, rows=len(metars_df), window_label=window_label)
