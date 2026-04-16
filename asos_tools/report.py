"""Dashboard-style reports built from 1-minute ASOS DataFrames.

Public API
----------
``build_report(df, *, window_label, station_id, station_name, out_path)``
    Render a multi-panel PNG report from a DataFrame produced by
    :func:`asos_tools.fetch.fetch_1min`.

The layout is a dark-theme dashboard:

    ┌─────────────────────────────────────────────────────────────┐
    │  Header  (station · window · key stat chips)                │
    ├──────────────────────────────────┬──────────────────────────┤
    │  Temperature + Dewpoint          │                          │
    │  (with filled spread + freezing  │       Wind Rose          │
    │   reference line)                │      (polar hist)        │
    ├──────────────────────────────────┤                          │
    │  Pressure                        │                          │
    ├──────────────────────────────────┴──────────────────────────┤
    │  Wind speed + gusts (full width)                            │
    ├─────────────────────────────────────────────────────────────┤
    │  Precipitation: per-interval bars + cumulative curve        │
    └─────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

import matplotlib.dates as mdates
import matplotlib.patheffects as patheffects
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec
from matplotlib.patches import FancyBboxPatch, Rectangle

__all__ = [
    "build_report",
    "build_maintenance_report",
    "build_comparison_report",
    "build_missing_report",
    "ReportResult",
]

# build_missing_report lives in _missing_report.py to avoid a 200-line
# append to this already-long file. Lazy-import here so the public API
# surface (from asos_tools.report import build_missing_report) works.
def __getattr__(name):
    if name == "build_missing_report":
        from asos_tools._missing_report import build_missing_report
        return build_missing_report
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# -----------------------------------------------------------------------------
# Theme
# -----------------------------------------------------------------------------

# Tailwind-ish dark palette — vivid-on-slate.
BG_OUTER = "#0b1220"
BG_PANEL = "#111a2e"
BG_CHIP = "#1b2740"
GRID = "#23314f"
BORDER = "#2d3d5c"
MUTED = "#94a3b8"
SOFT = "#cbd5e1"
FG = "#e2e8f0"
FG_HI = "#f8fafc"
ACCENT = "#38bdf8"  # sky-400

# Series colors
C_TEMP = "#f97316"  # orange-500
C_DEW = "#0ea5e9"  # sky-500
C_SPREAD = "#1e40af"  # blue-800
C_WIND = "#4ade80"  # green-400
C_GUST = "#c084fc"  # violet-400
C_PRES = "#fbbf24"  # amber-400
C_PRECIP = "#22d3ee"  # cyan-400
C_CUM = "#f472b6"  # pink-400
C_FREEZE = "#64748b"  # slate-500


def _apply_style() -> None:
    plt.rcParams.update({
        "figure.facecolor": BG_OUTER,
        "savefig.facecolor": BG_OUTER,
        "axes.facecolor": BG_PANEL,
        "axes.edgecolor": BORDER,
        "axes.labelcolor": FG,
        "axes.titlecolor": FG_HI,
        "axes.titlesize": 10.5,
        "axes.titleweight": "bold",
        "axes.titlepad": 10,
        "axes.titlelocation": "left",
        "axes.labelsize": 9,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.linewidth": 0.8,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "xtick.major.pad": 4,
        "ytick.major.pad": 4,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "grid.alpha": 0.55,
        "font.family": ["DejaVu Sans", "Segoe UI", "Arial"],
        "font.size": 9,
        "legend.frameon": False,
        "legend.fontsize": 8,
        "legend.labelcolor": SOFT,
    })


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _auto_time_axis(ax, valid: pd.Series) -> None:
    span = (valid.max() - valid.min()).total_seconds()
    if span <= 30 * 3600:        # up to ~1.25 days
        major = mdates.HourLocator(interval=3)
        fmt = mdates.DateFormatter("%H:%M")
    elif span <= 8 * 86400:      # up to ~8 days
        major = mdates.DayLocator()
        fmt = mdates.DateFormatter("%m-%d")
    elif span <= 20 * 86400:     # up to ~20 days
        major = mdates.DayLocator(interval=2)
        fmt = mdates.DateFormatter("%m-%d")
    else:                         # month-scale
        major = mdates.DayLocator(interval=5)
        fmt = mdates.DateFormatter("%m-%d")
    ax.xaxis.set_major_locator(major)
    ax.xaxis.set_major_formatter(fmt)


def _stat(value: float, fmt: str = "{:.1f}") -> str:
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return "—"
    return fmt.format(value)


def _draw_header(fig, ax, *, station_id: str, station_name: str,
                 window_label: str, df: pd.DataFrame) -> None:
    """Two-row header: title block on top, KPI chip strip across bottom."""
    ax.set_axis_off()

    # --- Title block (top ~55% of header strip) ---

    # Accent tag.
    ax.add_patch(Rectangle((0.0, 0.95), 0.035, 0.04,
                           transform=ax.transAxes, color=ACCENT,
                           clip_on=False, zorder=10))
    ax.text(0.045, 0.965, "1-MINUTE  OBSERVATION  REPORT",
            fontsize=8.5, color=ACCENT, fontweight="bold",
            transform=ax.transAxes, va="center")

    # Station name (auto-shrink if long).
    name_fs = 22 if len(station_name) <= 18 else (18 if len(station_name) <= 26 else 15)
    ax.text(0.0, 0.74, station_name, fontsize=name_fs, fontweight="bold",
            color=FG_HI, transform=ax.transAxes, va="center")

    # Sub-line.
    ax.text(0.0, 0.54,
            f"{station_id}   ·   {window_label.upper()}   ·   "
            f"{len(df):,} OBS   ·   "
            f"{df['valid'].min():%Y-%m-%d %H:%M}Z  →  {df['valid'].max():%Y-%m-%d %H:%M}Z",
            fontsize=9.2, color=SOFT, transform=ax.transAxes, va="center")

    # --- KPI chip strip (bottom ~42%) ---
    # Compute data completeness: actual rows vs expected 1-per-minute.
    span_min = (df["valid"].max() - df["valid"].min()).total_seconds() / 60
    completeness = round(len(df) / max(span_min, 1) * 100, 1) if span_min > 0 else 0

    chips: list[tuple[str, str, str]] = []
    if "tmpf" in df and df["tmpf"].notna().any():
        avg_t = df["tmpf"].mean()
        chips.append(("TEMP (min/avg/max)",
                      f"{_stat(df['tmpf'].min(),'{:.0f}')}/{_stat(avg_t,'{:.0f}')}/{_stat(df['tmpf'].max(),'{:.0f}')}°F",
                      C_TEMP))
    if "sknt" in df and df["sknt"].notna().any():
        avg_w = df["sknt"].mean()
        calm_pct = round((df["sknt"] == 0).sum() / max(df["sknt"].notna().sum(), 1) * 100, 0)
        chips.append(("WIND (avg/peak)",
                      f"{_stat(avg_w,'{:.0f}')}/{_stat(df['gust_sknt'].max(),'{:.0f}')} kt  {calm_pct:.0f}% calm",
                      C_WIND))
    if "pres1" in df and df["pres1"].notna().any():
        chips.append(("PRESSURE RANGE",
                      f"{_stat(df['pres1'].min(),'{:.2f}')}–{_stat(df['pres1'].max(),'{:.2f}')} inHg",
                      C_PRES))
    if "precip" in df and df["precip"].notna().any():
        chips.append(("TOTAL PRECIP",
                      f"{_stat(df['precip'].sum(),'{:.2f}')} in",
                      C_PRECIP))
    if "vis1_coeff" in df and df["vis1_coeff"].notna().any():
        min_vis = df["vis1_coeff"].min()
        chips.append(("DATA COMPLETENESS",
                      f"{completeness:.0f}% ({len(df):,} obs)",
                      ACCENT))

    _draw_chip_strip(ax, chips)


def _draw_chip_strip(ax, chips: list[tuple[str, str, str]]) -> None:
    """Render a horizontal strip of KPI chips along the bottom of ``ax``.

    Each chip is a tuple ``(label, value, accent_color)``. The label sits
    in the upper third of the chip; the value sits in the lower two-thirds.
    Chip height is chosen so neither element can visually overlap the other
    at the fontsizes we use below.
    """
    if not chips:
        return

    n = len(chips)
    gap = 0.012
    chip_w = (1.0 - gap * (n - 1)) / n
    chip_y = 0.035
    chip_h = 0.44  # taller than before so label + value never touch

    # Vertical zones inside the chip, as fractions of chip_h.
    label_y = chip_y + chip_h * 0.78   # upper strip
    value_y = chip_y + chip_h * 0.30   # lower strip

    for i, (label, value, accent) in enumerate(chips):
        x_left = i * (chip_w + gap)
        # Background.
        ax.add_patch(FancyBboxPatch(
            (x_left, chip_y), chip_w, chip_h,
            transform=ax.transAxes,
            boxstyle="round,pad=0.0,rounding_size=0.035",
            facecolor=BG_CHIP, edgecolor=BORDER, linewidth=0.9,
            clip_on=False,
        ))
        # Left accent stripe.
        ax.add_patch(Rectangle(
            (x_left, chip_y + 0.04), 0.005, chip_h - 0.08,
            transform=ax.transAxes, color=accent, clip_on=False,
        ))
        # Label (upper third, small, uppercase, muted).
        ax.text(x_left + 0.022, label_y, label,
                transform=ax.transAxes, fontsize=8, color=SOFT,
                ha="left", va="center")
        # Value (lower two-thirds, big, bright).
        ax.text(x_left + 0.022, value_y, value,
                transform=ax.transAxes, fontsize=15, fontweight="bold",
                color=FG_HI, ha="left", va="center")


def _annotate_extreme(ax, x, y, *, label: str, color: str, above: bool) -> None:
    """Annotate an extreme data point. Adjusts horizontal anchor near edges
    so the label never runs off the axis."""
    off_y = 13 if above else -13
    va = "bottom" if above else "top"

    # Detect proximity to axis edges using axes-fraction coords.
    ax_xmin, ax_xmax = ax.get_xlim()
    if ax_xmin != ax_xmax:
        x_num = mdates.date2num(x) if isinstance(x, (pd.Timestamp, datetime)) else x
        frac = (x_num - ax_xmin) / (ax_xmax - ax_xmin)
    else:
        frac = 0.5
    if frac < 0.08:
        ha = "left"
        off_x = 4
    elif frac > 0.92:
        ha = "right"
        off_x = -4
    else:
        ha = "center"
        off_x = 0

    t = ax.annotate(
        label,
        (x, y), textcoords="offset points", xytext=(off_x, off_y),
        fontsize=8, fontweight="bold", color=color,
        ha=ha, va=va,
        arrowprops=dict(arrowstyle="-", color=color, lw=0.8, alpha=0.7),
        path_effects=[patheffects.withStroke(linewidth=2.2, foreground=BG_PANEL)],
        annotation_clip=False,
    )
    t.set_clip_on(False)
    ax.plot([x], [y], "o", markersize=3.5, color=color,
            markeredgecolor=BG_PANEL, markeredgewidth=0.6, zorder=6,
            clip_on=False)


def _pad_ylim(ax, top_frac: float = 0.18, bottom_frac: float = 0.10) -> None:
    """Give the axis y-limits enough headroom so extreme-point annotations
    above and below the data always fit inside the panel."""
    lo, hi = ax.get_ylim()
    span = hi - lo
    if span <= 0:
        return
    ax.set_ylim(lo - span * bottom_frac, hi + span * top_frac)


def _panel_temp_dew(ax, df: pd.DataFrame) -> None:
    ax.set_title("TEMPERATURE · DEWPOINT", loc="left")
    valid = df["valid"]
    t = df.get("tmpf")
    d = df.get("dwpf")

    if t is not None and d is not None:
        ax.fill_between(valid, d, t, color=C_SPREAD, alpha=0.22,
                        linewidth=0, zorder=1, label="T–Td spread")
    if t is not None:
        ax.plot(valid, t, color=C_TEMP, lw=1.3, label="Temperature", zorder=3)
    if d is not None:
        ax.plot(valid, d, color=C_DEW, lw=1.3, label="Dewpoint", zorder=3)

    ax.set_ylabel("°F")
    ax.grid(True, axis="y", alpha=0.5)
    ax.margins(x=0.005)
    _pad_ylim(ax, top_frac=0.22, bottom_frac=0.15)
    _auto_time_axis(ax, valid)

    # Freezing reference — label anchored inside the axis on the LEFT
    # so it can never clip off the right edge.
    if t is not None and d is not None:
        lo_y, hi_y = ax.get_ylim()
        if lo_y < 32 < hi_y:
            ax.axhline(32, color=C_FREEZE, lw=0.7, ls=(0, (4, 3)),
                       alpha=0.7, zorder=2)
            ax.text(0.01, 32, "32°F", transform=ax.get_yaxis_transform(),
                    color=C_FREEZE, fontsize=7.5, va="center", ha="left",
                    alpha=0.9,
                    bbox=dict(facecolor=BG_PANEL, edgecolor="none",
                              boxstyle="round,pad=0.15"))

    # Annotate extremes AFTER ylim is padded so arrows stay inside.
    if t is not None and t.notna().any():
        i_hi = t.idxmax()
        i_lo = t.idxmin()
        _annotate_extreme(ax, valid.loc[i_hi], t.loc[i_hi],
                          label=f"{t.loc[i_hi]:.0f}°", color=C_TEMP, above=True)
        _annotate_extreme(ax, valid.loc[i_lo], t.loc[i_lo],
                          label=f"{t.loc[i_lo]:.0f}°", color=C_TEMP, above=False)

    ax.legend(loc="lower left", ncol=3, handlelength=1.6,
              fontsize=8, bbox_to_anchor=(0.0, -0.02))

    # Inline stats box — top-right corner.
    if t is not None and t.notna().any():
        mean_t = t.mean()
        spread = (t - d).mean() if d is not None and d.notna().any() else None
        stats_lines = [f"Avg: {mean_t:.1f}°F"]
        if spread is not None:
            stats_lines.append(f"Avg T-Td: {spread:.1f}°F")
        stats_text = "\n".join(stats_lines)
        ax.text(0.99, 0.97, stats_text, transform=ax.transAxes,
                fontsize=7.5, color=SOFT, ha="right", va="top",
                family="monospace",
                bbox=dict(facecolor=BG_CHIP, edgecolor=BORDER,
                          boxstyle="round,pad=0.3", alpha=0.9))


def _panel_pressure(ax, df: pd.DataFrame) -> None:
    ax.set_title("STATION PRESSURE", loc="left")
    valid = df["valid"]
    if "pres1" not in df or df["pres1"].notna().sum() == 0:
        ax.text(0.5, 0.5, "no pressure data", transform=ax.transAxes,
                ha="center", va="center", color=MUTED)
        ax.set_axis_off()
        return

    p = df["pres1"]
    ax.fill_between(valid, p.min(), p, color=C_PRES, alpha=0.12, linewidth=0)
    ax.plot(valid, p, color=C_PRES, lw=1.3)

    ax.set_ylabel("inHg")
    ax.grid(True, axis="y", alpha=0.5)
    ax.margins(x=0.005)
    _pad_ylim(ax, top_frac=0.22, bottom_frac=0.18)
    _auto_time_axis(ax, valid)

    i_lo = p.idxmin()
    i_hi = p.idxmax()
    if pd.notna(i_hi):
        _annotate_extreme(ax, valid.loc[i_hi], p.loc[i_hi],
                          label=f"{p.loc[i_hi]:.2f}", color=C_PRES, above=True)
    if pd.notna(i_lo):
        _annotate_extreme(ax, valid.loc[i_lo], p.loc[i_lo],
                          label=f"{p.loc[i_lo]:.2f}", color=C_PRES, above=False)

    # Net-change pill inside the panel, clearly on-axis.
    valid_pres = p.dropna()
    if len(valid_pres) >= 2:
        delta = valid_pres.iloc[-1] - valid_pres.iloc[0]
        arrow = "↑" if delta > 0.02 else ("↓" if delta < -0.02 else "→")
        color = "#4ade80" if delta > 0.02 else ("#f87171" if delta < -0.02 else MUTED)
        ax.text(
            0.985, 0.92,
            f"{arrow} {delta:+.2f} inHg",
            transform=ax.transAxes, fontsize=9.5, fontweight="bold",
            color=color, ha="right", va="top",
            bbox=dict(facecolor=BG_CHIP, edgecolor=BORDER,
                      boxstyle="round,pad=0.3"),
        )


SPEED_BINS = [0, 5, 10, 15, 20, 30, 100]
SPEED_COLORS = ["#0ea5e9", "#22d3ee", "#4ade80", "#facc15",
                "#f97316", "#ef4444"]
SPEED_LABELS = ["0–5", "5–10", "10–15", "15–20", "20–30", ">30"]


def _panel_wind_rose(ax_polar, ax_legend, df: pd.DataFrame) -> None:
    """Circular histogram of wind direction stacked by speed bin.

    Takes two axes so the legend can live in its own dedicated strip
    below the rose, avoiding overlap with adjacent panels.
    """
    ax_polar.set_title("WIND ROSE", pad=10, loc="center", fontsize=10)
    ax_polar.set_facecolor(BG_PANEL)
    ax_legend.set_axis_off()

    d = df[["drct", "sknt"]].dropna()
    d = d[(d["sknt"] > 0) & (d["drct"] >= 0)]
    if d.empty:
        ax_polar.text(0.5, 0.5, "no wind data", transform=ax_polar.transAxes,
                      ha="center", va="center", color=MUTED)
        return

    dir_bins = np.arange(0, 361, 22.5)
    theta = np.deg2rad(0.5 * (dir_bins[:-1] + dir_bins[1:]))
    width = np.deg2rad(22.5)
    bottom = np.zeros(len(theta))
    total = len(d)

    for (lo, hi), color in zip(zip(SPEED_BINS[:-1], SPEED_BINS[1:]), SPEED_COLORS):
        mask = (d["sknt"] >= lo) & (d["sknt"] < hi)
        counts, _ = np.histogram(d.loc[mask, "drct"] % 360, bins=dir_bins)
        pct = counts / max(total, 1) * 100
        ax_polar.bar(theta, pct, width=width, bottom=bottom,
                     color=color, edgecolor=BG_PANEL, linewidth=0.5, zorder=3)
        bottom = bottom + pct

    # Cardinal labels — keep them close to the rose so they fit the cell.
    ax_polar.set_theta_zero_location("N")
    ax_polar.set_theta_direction(-1)
    ax_polar.set_xticks(np.deg2rad(np.arange(0, 360, 45)))
    ax_polar.set_xticklabels(["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
                             color=FG, fontweight="bold", fontsize=8.5)
    ax_polar.tick_params(axis="x", pad=2)

    # Radial % rings — labels inside the rose.
    rmax = bottom.max() if bottom.max() > 0 else 5
    ring_step = max(1, int(np.ceil(rmax / 4)))
    rings = np.arange(ring_step, rmax + ring_step, ring_step)
    ax_polar.set_rticks(rings)
    ax_polar.set_yticklabels([f"{int(r)}%" for r in rings],
                             fontsize=6.5, color=MUTED)
    ax_polar.set_rlabel_position(135)  # move radial labels away from N/E
    ax_polar.tick_params(axis="y", pad=0)
    ax_polar.spines["polar"].set_color(BORDER)
    ax_polar.grid(True, color=GRID, alpha=0.55, linewidth=0.55)

    # ---- Custom legend strip below the rose -----------------------------
    ax_legend.set_xlim(0, 1)
    ax_legend.set_ylim(0, 1)
    ax_legend.text(0.5, 0.88, "WIND SPEED  (kt)",
                   transform=ax_legend.transAxes, color=MUTED,
                   fontsize=7, ha="center", va="top")
    n = len(SPEED_LABELS)
    slot_w = 0.92 / n
    x_start = 0.04
    for i, (color, label) in enumerate(zip(SPEED_COLORS, SPEED_LABELS)):
        cx = x_start + (i + 0.5) * slot_w
        # Color square.
        ax_legend.add_patch(Rectangle(
            (cx - 0.018, 0.38), 0.036, 0.22,
            transform=ax_legend.transAxes,
            facecolor=color, edgecolor="none",
        ))
        # Label below the square.
        ax_legend.text(cx, 0.15, label,
                       transform=ax_legend.transAxes,
                       color=SOFT, fontsize=7, ha="center", va="center")


def _panel_wind(ax, df: pd.DataFrame) -> None:
    ax.set_title("WIND SPEED · GUSTS", loc="left")
    valid = df["valid"]
    if "sknt" in df:
        ax.fill_between(valid, 0, df["sknt"], color=C_WIND,
                        alpha=0.22, linewidth=0, zorder=1)
        ax.plot(valid, df["sknt"], color=C_WIND, lw=1.2,
                label="Wind (2-min mean)", zorder=3)
    if "gust_sknt" in df:
        ax.plot(valid, df["gust_sknt"], color=C_GUST, lw=0.9,
                alpha=0.95, label="Gust (1-min peak)", zorder=2)

    ax.set_ylabel("knots")
    ax.grid(True, axis="y", alpha=0.5)
    ax.margins(x=0.005)
    ax.set_ylim(bottom=0)
    _pad_ylim(ax, top_frac=0.25, bottom_frac=0.0)
    _auto_time_axis(ax, valid)

    # Annotate peak AFTER ylim padding.
    if "gust_sknt" in df and df["gust_sknt"].notna().any():
        idx = df["gust_sknt"].idxmax()
        _annotate_extreme(ax, valid.loc[idx], df["gust_sknt"].loc[idx],
                          label=f"{df['gust_sknt'].loc[idx]:.0f} kt",
                          color=C_GUST, above=True)

    ax.legend(loc="upper left", ncol=2, handlelength=1.6, fontsize=8)

    # Inline stats box.
    if "sknt" in df and df["sknt"].notna().any():
        avg_spd = df["sknt"].mean()
        max_gust = df["gust_sknt"].max() if "gust_sknt" in df else None
        calm_n = (df["sknt"] == 0).sum()
        calm_pct = calm_n / max(df["sknt"].notna().sum(), 1) * 100
        lines = [f"Avg: {avg_spd:.1f} kt", f"Calm: {calm_pct:.0f}%"]
        if max_gust and pd.notna(max_gust):
            lines.append(f"Peak: {max_gust:.0f} kt")
        ax.text(0.99, 0.97, "\n".join(lines), transform=ax.transAxes,
                fontsize=7.5, color=SOFT, ha="right", va="top",
                family="monospace",
                bbox=dict(facecolor=BG_CHIP, edgecolor=BORDER,
                          boxstyle="round,pad=0.3", alpha=0.9))


def _choose_precip_freq(span_seconds: float) -> tuple[str, str]:
    """Pick a sensible resample frequency & label."""
    if span_seconds <= 2 * 86400:       # 1 day: 15-min bars
        return "15min", "in / 15-min"
    if span_seconds <= 10 * 86400:      # 7-day: hourly bars
        return "1h", "in / hour"
    if span_seconds <= 20 * 86400:      # 14-day: 3-hour bars
        return "3h", "in / 3-hour"
    return "6h", "in / 6-hour"          # 30-day: 6-hour bars


def _panel_precip(ax, df: pd.DataFrame) -> None:
    ax.set_title("PRECIPITATION", loc="left")
    valid = df["valid"]
    if "precip" not in df or df["precip"].notna().sum() == 0:
        ax.text(0.5, 0.5, "no precip data", transform=ax.transAxes,
                ha="center", va="center", color=MUTED)
        ax.set_axis_off()
        return

    span = (valid.max() - valid.min()).total_seconds()
    freq, unit = _choose_precip_freq(span)

    series = df.set_index("valid")["precip"].resample(freq).sum(min_count=1)
    width_days = pd.Timedelta(freq).total_seconds() / 86400

    # Per-interval bars on primary axis.
    ax.bar(series.index, series.values, width=width_days, align="edge",
           color=C_PRECIP, edgecolor="none", alpha=0.9)
    ax.set_ylabel(unit, color=C_PRECIP, fontsize=8.5)
    ax.tick_params(axis="y", colors=C_PRECIP, labelsize=8)
    ax.grid(True, axis="y", alpha=0.5)
    ax.margins(x=0.005)
    # Reserve headroom so the bars don't kiss the top frame.
    bar_max = float(series.max(skipna=True) or 0)
    ax.set_ylim(0, bar_max * 1.25 if bar_max > 0 else 1.0)

    # Cumulative overlay on twin axis, with its own headroom.
    cumulative = series.fillna(0).cumsum()
    total = float(df["precip"].sum(skipna=True))
    ax2 = ax.twinx()
    ax2.plot(cumulative.index, cumulative.values, color=C_CUM, lw=1.6,
             zorder=5)
    ax2.fill_between(cumulative.index, 0, cumulative.values,
                     color=C_CUM, alpha=0.10, linewidth=0)
    ax2.set_ylabel("cumulative (in)", color=C_CUM, fontsize=8.5)
    ax2.tick_params(axis="y", colors=C_CUM, labelsize=8)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color(BORDER)
    ax2.grid(False)
    ax2.set_ylim(0, max(total * 1.15, 0.05))

    # Unified legend, top-left.
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=C_PRECIP, alpha=0.9),
        plt.Line2D([0], [0], color=C_CUM, lw=1.6),
    ]
    ax.legend(handles, [unit, "cumulative"], loc="upper left",
              ncol=2, handlelength=1.6, fontsize=8)

    # Total callout — styled pill at top-right (inside both axes).
    ax.text(0.985, 0.92, f"{total:.2f} in",
            transform=ax.transAxes, fontsize=11, fontweight="bold",
            color=FG_HI, ha="right", va="top",
            bbox=dict(facecolor=BG_CHIP, edgecolor=BORDER,
                      boxstyle="round,pad=0.3"))

    _auto_time_axis(ax, valid)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

@dataclass
class ReportResult:
    path: Path
    rows: int
    window_label: str


def build_report(
    df: pd.DataFrame,
    *,
    window_label: str,
    station_id: str,
    station_name: str | None = None,
    out_path: Union[str, Path],
) -> ReportResult:
    """Render a full dashboard report to ``out_path`` (PNG).

    Parameters
    ----------
    df
        Output of :func:`asos_tools.fetch.fetch_1min`.
    window_label
        Human label shown in the header (e.g. ``"1 day"``, ``"7 day"``).
    station_id
        Station identifier (e.g. ``"KJFK"``).
    station_name
        Optional pretty name; inferred from the DataFrame if omitted.
    out_path
        Destination PNG path. Parent directories are created.
    """
    if df.empty:
        raise ValueError("Cannot build report from an empty DataFrame")

    _apply_style()
    name = station_name or (df["station_name"].iloc[0] if "station_name" in df else station_id)

    fig = plt.figure(figsize=(14, 11), dpi=120)
    gs = GridSpec(
        5, 12,
        height_ratios=[1.1, 1.55, 1.25, 1.65, 1.35],
        hspace=0.7, wspace=0.4,
        left=0.055, right=0.97, top=0.96, bottom=0.06,
    )

    # Row 0 — header strip spans all columns.
    ax_head = fig.add_subplot(gs[0, :])
    _draw_header(fig, ax_head, station_id=station_id, station_name=name,
                 window_label=window_label, df=df)

    # Row 1 — temperature + dewpoint (full width).
    ax_temp = fig.add_subplot(gs[1, :])
    _panel_temp_dew(ax_temp, df)

    # Row 2 — pressure (full width).
    ax_pres = fig.add_subplot(gs[2, :])
    _panel_pressure(ax_pres, df)

    # Row 3 — wind speed (left 8/12) + wind rose (right 4/12 split into rose+legend).
    ax_wind = fig.add_subplot(gs[3, 0:8])
    _panel_wind(ax_wind, df)

    rose_spec = gs[3, 8:12].subgridspec(2, 1, height_ratios=[4.5, 1], hspace=0.05)
    ax_rose = fig.add_subplot(rose_spec[0], projection="polar")
    ax_rose_legend = fig.add_subplot(rose_spec[1])
    _panel_wind_rose(ax_rose, ax_rose_legend, df)

    # Row 4 — precipitation (full width).
    ax_precip = fig.add_subplot(gs[4, :])
    _panel_precip(ax_precip, df)

    # Footer — data source + generation timestamp.
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    fig.text(0.055, 0.018,
             f"DATA  NOAA/NCEI ASOS 1-minute archive  via  Iowa Environmental Mesonet",
             fontsize=7.2, color=MUTED, ha="left")
    fig.text(0.97, 0.018,
             f"GENERATED  {now}  ·  asos-tools-py",
             fontsize=7.2, color=MUTED, ha="right")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120, bbox_inches="tight", facecolor=BG_OUTER)
    plt.close(fig)
    return ReportResult(path=out, rows=len(df), window_label=window_label)


# =============================================================================
# Maintenance-flag report
# =============================================================================

C_FLAG = "#f87171"   # red-400 — flagged (degraded) observations
C_CLEAN = "#34d399"  # emerald-400 — clean observations


def _draw_maint_header(ax, *, group_label: str, window_label: str,
                       metars_df: pd.DataFrame) -> None:
    ax.set_axis_off()
    ax.add_patch(Rectangle((0.0, 0.95), 0.035, 0.04,
                           transform=ax.transAxes, color=C_FLAG,
                           clip_on=False, zorder=10))
    ax.text(0.045, 0.965, "ASOS  MAINTENANCE-FLAG  REPORT",
            fontsize=8.5, color=C_FLAG, fontweight="bold",
            transform=ax.transAxes, va="center")
    ax.text(0.0, 0.74, group_label, fontsize=22, fontweight="bold",
            color=FG_HI, transform=ax.transAxes, va="center")

    n_total = len(metars_df)
    n_flagged = int(metars_df["has_maintenance"].sum())
    n_stations = metars_df["station"].nunique()
    rate = (n_flagged / n_total * 100) if n_total else 0

    ax.text(0.0, 0.55,
            f"{window_label.upper()}   ·   "
            f"{n_stations} STATIONS   ·   "
            f"{n_total:,} METARS   ·   "
            f"{metars_df['valid'].min():%Y-%m-%d %H:%M}Z  →  "
            f"{metars_df['valid'].max():%Y-%m-%d %H:%M}Z",
            fontsize=9.2, color=SOFT, transform=ax.transAxes, va="center")

    _draw_chip_strip(ax, [
        ("FLAG RATE",      f"{rate:.1f}%",          C_FLAG),
        ("FLAGGED METARS", f"{n_flagged:,}",        C_FLAG),
        ("CLEAN METARS",   f"{n_total - n_flagged:,}", C_CLEAN),
        ("STATIONS",       f"{n_stations}",         ACCENT),
    ])


def _panel_flag_rate_per_station(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("FLAG RATE  BY  STATION", loc="left")
    per_stn = metars_df.groupby("station")["has_maintenance"].agg(["mean", "count"])
    per_stn["rate"] = per_stn["mean"] * 100
    per_stn = per_stn.sort_values("rate")

    # Compute probable reason per station from the last flagged METAR.
    from asos_tools.metars import decode_reasons_short
    reasons: dict[str, str] = {}
    for stn in per_stn.index:
        flagged = metars_df[(metars_df["station"] == stn) & metars_df["has_maintenance"]]
        if not flagged.empty:
            row = flagged.iloc[-1]
            wx = row.get("wxcodes") if "wxcodes" in flagged.columns else None
            reasons[stn] = decode_reasons_short(row["metar"], wx)
        else:
            reasons[stn] = ""

    colors = [C_CLEAN if r < 10 else ("#fbbf24" if r < 50 else C_FLAG)
              for r in per_stn["rate"]]
    bars = ax.barh(per_stn.index, per_stn["rate"],
                   color=colors, edgecolor="none", height=0.72)
    ax.set_xlim(0, 105)
    ax.set_xlabel("% of METARs ending in $")
    ax.grid(True, axis="x", alpha=0.4)
    for bar, (stn, row_data) in zip(bars, per_stn.iterrows()):
        label = f"{row_data['rate']:.0f}%  ({row_data['count']})"
        reason = reasons.get(stn, "")
        if reason:
            label += f"  — {reason[:40]}"
        ax.text(row_data["rate"] + 1, bar.get_y() + bar.get_height() / 2,
                label, va="center", color=SOFT, fontsize=7)


def _panel_flag_heatmap(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("FLAG HEATMAP  (station × time)", loc="left")
    if metars_df.empty:
        ax.set_axis_off()
        return

    # Choose a temporal bin granularity based on window length.
    span_hours = (metars_df["valid"].max() - metars_df["valid"].min()).total_seconds() / 3600
    if span_hours <= 48:
        bin_spec, fmt = "1h", "%m-%d %Hh"
    elif span_hours <= 24 * 8:
        bin_spec, fmt = "3h", "%m-%d %Hh"
    elif span_hours <= 24 * 20:
        bin_spec, fmt = "12h", "%m-%d"
    else:
        bin_spec, fmt = "1D", "%m-%d"

    df = metars_df.copy()
    df["bin"] = df["valid"].dt.floor(bin_spec)
    piv = (df.groupby(["station", "bin"])["has_maintenance"].mean()
             .unstack("bin"))
    piv = piv.reindex(sorted(piv.index))

    im = ax.imshow(piv.values, aspect="auto", cmap="RdYlGn_r",
                   vmin=0, vmax=1, interpolation="nearest")
    ax.set_yticks(range(len(piv.index)))
    ax.set_yticklabels(piv.index, fontsize=8, color=FG)
    ticks = np.linspace(0, len(piv.columns) - 1, min(10, len(piv.columns))).astype(int)
    ax.set_xticks(ticks)
    ax.set_xticklabels([piv.columns[i].strftime(fmt) for i in ticks],
                       fontsize=7, color=MUTED, rotation=0)
    cbar = plt.colorbar(im, ax=ax, pad=0.01, aspect=14)
    cbar.set_label("flag rate", color=MUTED, fontsize=8)
    cbar.ax.tick_params(labelsize=7, colors=MUTED)


def _panel_flag_timeline(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("HOURLY  FLAG  RATE", loc="left")
    if metars_df.empty:
        ax.set_axis_off()
        return
    s = (metars_df.set_index("valid")["has_maintenance"]
         .resample("1h").mean() * 100)
    ax.fill_between(s.index, 0, s.values, color=C_FLAG, alpha=0.25, linewidth=0)
    ax.plot(s.index, s.values, color=C_FLAG, lw=1.0)
    ax.set_ylabel("% flagged / hr")
    ax.set_ylim(0, 105)
    ax.grid(True, axis="y", alpha=0.4)
    _auto_time_axis(ax, metars_df["valid"])


def build_maintenance_report(
    metars_df: pd.DataFrame,
    *,
    group_label: str,
    window_label: str,
    out_path: Union[str, Path],
) -> ReportResult:
    """Render a maintenance-flag dashboard.

    ``metars_df`` must be the output of :func:`asos_tools.fetch_metars`
    (i.e. have columns ``station``, ``valid``, ``has_maintenance``).
    """
    if metars_df.empty:
        raise ValueError("Cannot build maintenance report from empty DataFrame")

    _apply_style()
    fig = plt.figure(figsize=(14, 10), dpi=120)
    gs = GridSpec(
        4, 2,
        height_ratios=[1.15, 1.9, 1.5, 1.3],
        hspace=0.55, wspace=0.22,
        left=0.055, right=0.97, top=0.96, bottom=0.06,
    )

    ax_head = fig.add_subplot(gs[0, :])
    _draw_maint_header(ax_head, group_label=group_label,
                       window_label=window_label, metars_df=metars_df)

    ax_rate = fig.add_subplot(gs[1, 0])
    _panel_flag_rate_per_station(ax_rate, metars_df)

    ax_heat = fig.add_subplot(gs[1, 1])
    _panel_flag_heatmap(ax_heat, metars_df)

    ax_time = fig.add_subplot(gs[2, :])
    _panel_flag_timeline(ax_time, metars_df)

    # Panel: sample flagged METARs (bottom row).
    ax_samp = fig.add_subplot(gs[3, :])
    ax_samp.set_axis_off()
    ax_samp.set_title("RECENT  FLAGGED  METARS", loc="left")
    flagged = metars_df[metars_df["has_maintenance"]].tail(6)
    if flagged.empty:
        ax_samp.text(0.5, 0.5, "no flagged reports in window",
                     transform=ax_samp.transAxes, ha="center",
                     va="center", color=MUTED)
    else:
        ax_samp.add_patch(Rectangle(
            (0.0, 0.05), 1.0, 0.78, transform=ax_samp.transAxes,
            facecolor=BG_CHIP, edgecolor=BORDER, linewidth=0.8,
        ))
        for i, (_, row) in enumerate(flagged.iterrows()):
            y = 0.75 - i * 0.12
            ax_samp.text(0.012, y,
                         f"{row['valid']:%m-%d %H:%M}Z",
                         transform=ax_samp.transAxes, fontsize=8,
                         color=MUTED, family="monospace")
            ax_samp.text(0.095, y, row["station"],
                         transform=ax_samp.transAxes, fontsize=8.5,
                         color=FG_HI, fontweight="bold", family="monospace")
            ax_samp.text(0.14, y,
                         row["metar"][:140] + ("…" if len(row["metar"]) > 140 else ""),
                         transform=ax_samp.transAxes, fontsize=7.8,
                         color=SOFT, family="monospace")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    fig.text(0.055, 0.018,
             "DATA  NOAA/NCEI ASOS METAR archive  via  Iowa Environmental Mesonet",
             fontsize=7.2, color=MUTED, ha="left")
    fig.text(0.97, 0.018,
             f"GENERATED  {now}  ·  asos-tools-py",
             fontsize=7.2, color=MUTED, ha="right")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120, bbox_inches="tight", facecolor=BG_OUTER)
    plt.close(fig)
    return ReportResult(path=out, rows=len(metars_df), window_label=window_label)


# =============================================================================
# Comparison report: $ vs clean
# =============================================================================

def _draw_cmp_header(ax, *, group_label: str, window_label: str,
                     metars_df: pd.DataFrame) -> None:
    ax.set_axis_off()
    ax.add_patch(Rectangle((0.0, 0.95), 0.035, 0.04,
                           transform=ax.transAxes, color=ACCENT,
                           clip_on=False, zorder=10))
    ax.text(0.045, 0.965, "FLAGGED  VS  CLEAN  COMPARISON",
            fontsize=8.5, color=ACCENT, fontweight="bold",
            transform=ax.transAxes, va="center")
    ax.text(0.0, 0.74, group_label, fontsize=22, fontweight="bold",
            color=FG_HI, transform=ax.transAxes, va="center")

    n_total = len(metars_df)
    n_flag = int(metars_df["has_maintenance"].sum())
    n_clean = n_total - n_flag
    rate = (n_flag / n_total * 100) if n_total else 0

    ax.text(0.0, 0.55,
            f"{window_label.upper()}   ·   "
            f"{metars_df['station'].nunique()} STATIONS   ·   "
            f"{n_total:,} METARS  "
            f"({n_clean:,} CLEAN  +  {n_flag:,} FLAGGED)",
            fontsize=9.2, color=SOFT, transform=ax.transAxes, va="center")

    _draw_chip_strip(ax, [
        ("CLEAN (NO $)",  f"{n_clean:,}",                   C_CLEAN),
        ("FLAGGED ($)",   f"{n_flag:,}",                    C_FLAG),
        ("FLAG RATE",     f"{rate:.1f}%",                   C_FLAG),
        ("DELTA RATIO",   f"{(n_flag/max(n_clean,1)):.2f}x", ACCENT),
    ])


def _panel_cmp_stacked_over_time(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("FLAGGED  vs  CLEAN  OVER  TIME", loc="left")
    if metars_df.empty:
        ax.set_axis_off()
        return
    span_hours = (metars_df["valid"].max() - metars_df["valid"].min()).total_seconds() / 3600
    freq = "1h" if span_hours <= 48 else ("3h" if span_hours <= 8 * 24 else "12h")

    df = metars_df.copy()
    df["bin"] = df["valid"].dt.floor(freq)
    counts = (df.groupby(["bin", "has_maintenance"])
                .size().unstack(fill_value=0)
                .rename(columns={True: "flagged", False: "clean"}))
    for c in ("clean", "flagged"):
        if c not in counts.columns:
            counts[c] = 0
    counts = counts[["clean", "flagged"]]

    width_days = pd.Timedelta(freq).total_seconds() / 86400
    ax.bar(counts.index, counts["clean"], width=width_days,
           color=C_CLEAN, align="edge", label="clean", edgecolor="none", alpha=0.95)
    ax.bar(counts.index, counts["flagged"], width=width_days,
           bottom=counts["clean"],
           color=C_FLAG, align="edge", label="flagged $",
           edgecolor="none", alpha=0.95)
    ax.set_ylabel("METARs / bin")
    ax.legend(loc="upper left", ncol=2, handlelength=1.4)
    ax.grid(True, axis="y", alpha=0.4)
    _auto_time_axis(ax, metars_df["valid"])


def _panel_cmp_per_station(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("PER-STATION  CLEAN  vs  FLAGGED", loc="left")
    g = (metars_df.groupby(["station", "has_maintenance"])
                  .size().unstack(fill_value=0)
                  .rename(columns={True: "flagged", False: "clean"}))
    for c in ("clean", "flagged"):
        if c not in g.columns:
            g[c] = 0
    g["total"] = g["clean"] + g["flagged"]
    g = g.sort_values("total")

    y = np.arange(len(g.index))
    ax.barh(y, g["clean"], color=C_CLEAN, height=0.7, label="clean")
    ax.barh(y, g["flagged"], left=g["clean"], color=C_FLAG,
            height=0.7, label="flagged $")
    ax.set_yticks(y)
    ax.set_yticklabels(g.index, fontsize=8, color=FG)
    ax.set_xlabel("METAR count")
    ax.legend(loc="lower right", ncol=2, handlelength=1.4)
    ax.grid(True, axis="x", alpha=0.4)


def _panel_cmp_hour_of_day(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("FLAG  RATE  BY  HOUR  OF  DAY", loc="left")
    if metars_df.empty:
        ax.set_axis_off()
        return
    by_hour = (metars_df.assign(hour=lambda d: d["valid"].dt.hour)
                        .groupby("hour")["has_maintenance"]
                        .mean() * 100).reindex(range(24), fill_value=0)
    ax.bar(by_hour.index, by_hour.values, color=C_FLAG, alpha=0.85,
           edgecolor="none", width=0.8)
    ax.set_xticks(range(0, 24, 3))
    ax.set_xlabel("hour (UTC)")
    ax.set_ylabel("% flagged")
    ax.set_ylim(0, 105)
    ax.grid(True, axis="y", alpha=0.4)


def build_comparison_report(
    metars_df: pd.DataFrame,
    *,
    group_label: str,
    window_label: str,
    out_path: Union[str, Path],
) -> ReportResult:
    """Render a flagged-vs-clean comparison dashboard for a set of METARs."""
    if metars_df.empty:
        raise ValueError("Cannot build comparison report from empty DataFrame")

    _apply_style()
    fig = plt.figure(figsize=(14, 10), dpi=120)
    gs = GridSpec(
        4, 2,
        height_ratios=[1.15, 1.8, 1.6, 1.3],
        hspace=0.55, wspace=0.22,
        left=0.055, right=0.97, top=0.96, bottom=0.06,
    )

    ax_head = fig.add_subplot(gs[0, :])
    _draw_cmp_header(ax_head, group_label=group_label,
                     window_label=window_label, metars_df=metars_df)

    ax_time = fig.add_subplot(gs[1, :])
    _panel_cmp_stacked_over_time(ax_time, metars_df)

    ax_stn = fig.add_subplot(gs[2, 0])
    _panel_cmp_per_station(ax_stn, metars_df)

    ax_hod = fig.add_subplot(gs[2, 1])
    _panel_cmp_hour_of_day(ax_hod, metars_df)

    # Bottom: small text block with interpretation.
    ax_txt = fig.add_subplot(gs[3, :])
    ax_txt.set_axis_off()
    ax_txt.set_title("INTERPRETATION", loc="left")
    flag_stations = (metars_df.groupby("station")["has_maintenance"].mean()
                              .sort_values(ascending=False))
    worst = flag_stations.iloc[:3]
    best = flag_stations[flag_stations == flag_stations.min()].index.tolist()[:3]

    lines = [
        f"•  Highest flag rates:  " +
        ", ".join(f"{s} ({r*100:.0f}%)" for s, r in worst.items()),
        f"•  Clean (0% flagged):  " + (", ".join(best) if best else "—"),
        "•  The $ maintenance indicator signals the ASOS self-test detected "
        "an out-of-tolerance condition. Check the Probable Reason column for details.",
    ]
    for i, line in enumerate(lines):
        ax_txt.text(0.005, 0.72 - i * 0.26, line,
                    transform=ax_txt.transAxes, fontsize=9.5,
                    color=SOFT, va="top")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    fig.text(0.055, 0.018,
             "DATA  NOAA/NCEI ASOS METAR archive  via  Iowa Environmental Mesonet",
             fontsize=7.2, color=MUTED, ha="left")
    fig.text(0.97, 0.018,
             f"GENERATED  {now}  ·  asos-tools-py",
             fontsize=7.2, color=MUTED, ha="right")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120, bbox_inches="tight", facecolor=BG_OUTER)
    plt.close(fig)
    return ReportResult(path=out, rows=len(metars_df), window_label=window_label)
