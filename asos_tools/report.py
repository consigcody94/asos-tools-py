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
    "ReportResult",
]


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
    chips: list[tuple[str, str, str]] = []
    if "tmpf" in df and df["tmpf"].notna().any():
        chips.append(("TEMP RANGE",
                      f"{_stat(df['tmpf'].min(),'{:.0f}')}–{_stat(df['tmpf'].max(),'{:.0f}')}°F",
                      C_TEMP))
    if "gust_sknt" in df and df["gust_sknt"].notna().any():
        chips.append(("PEAK GUST",
                      f"{_stat(df['gust_sknt'].max(),'{:.0f}')} kt",
                      C_GUST))
    if "pres1" in df and df["pres1"].notna().any():
        chips.append(("PRESSURE RANGE",
                      f"{_stat(df['pres1'].min(),'{:.2f}')}–{_stat(df['pres1'].max(),'{:.2f}')}",
                      C_PRES))
    if "precip" in df and df["precip"].notna().any():
        chips.append(("TOTAL PRECIP",
                      f"{_stat(df['precip'].sum(),'{:.2f}')} in",
                      C_PRECIP))

    if not chips:
        return

    n = len(chips)
    chip_y = 0.04
    chip_h = 0.38
    gap = 0.012
    chip_w = (1.0 - gap * (n - 1)) / n

    for i, (label, value, accent) in enumerate(chips):
        x_left = i * (chip_w + gap)
        # Background chip.
        chip = FancyBboxPatch((x_left, chip_y), chip_w, chip_h,
                              transform=ax.transAxes,
                              boxstyle="round,pad=0.0,rounding_size=0.04",
                              facecolor=BG_CHIP, edgecolor=BORDER,
                              linewidth=0.9, clip_on=False)
        ax.add_patch(chip)
        # Left accent stripe.
        stripe_w = 0.005
        stripe = Rectangle((x_left, chip_y + 0.04),
                           stripe_w, chip_h - 0.08,
                           transform=ax.transAxes,
                           color=accent, clip_on=False)
        ax.add_patch(stripe)
        # Label (top, small, muted).
        ax.text(x_left + 0.020, chip_y + chip_h - 0.06, label,
                transform=ax.transAxes, fontsize=7.4, color=MUTED,
                ha="left", va="top")
        # Value (bottom, big, bright).
        ax.text(x_left + 0.020, chip_y + 0.07, value,
                transform=ax.transAxes, fontsize=16, fontweight="bold",
                color=FG_HI, ha="left", va="bottom")


def _annotate_extreme(ax, x, y, *, label: str, color: str, above: bool) -> None:
    off = (0, 14) if above else (0, -14)
    va = "bottom" if above else "top"
    ax.annotate(
        label,
        (x, y), textcoords="offset points", xytext=off,
        fontsize=8, fontweight="bold", color=color,
        ha="center", va=va,
        arrowprops=dict(arrowstyle="-", color=color, lw=0.8, alpha=0.7),
        path_effects=[patheffects.withStroke(linewidth=2.2, foreground=BG_PANEL)],
    )
    ax.plot([x], [y], "o", markersize=3.5, color=color,
            markeredgecolor=BG_PANEL, markeredgewidth=0.6, zorder=6)


def _panel_temp_dew(ax, df: pd.DataFrame) -> None:
    ax.set_title("TEMPERATURE · DEWPOINT", loc="left")
    valid = df["valid"]
    t = df.get("tmpf")
    d = df.get("dwpf")

    if t is not None and d is not None:
        # Fill the T–Td spread (dewpoint depression).
        ax.fill_between(valid, d, t, color=C_SPREAD, alpha=0.22,
                        linewidth=0, zorder=1, label="T–Td spread")
    if t is not None:
        ax.plot(valid, t, color=C_TEMP, lw=1.1, label="Temperature", zorder=3)
    if d is not None:
        ax.plot(valid, d, color=C_DEW, lw=1.1, label="Dewpoint", zorder=3)

    # Freezing reference.
    ax.axhline(32, color=C_FREEZE, lw=0.7, ls=(0, (4, 3)), alpha=0.7, zorder=2)
    ax.text(valid.iloc[-1], 32, " 32°F", color=C_FREEZE,
            fontsize=7.5, va="center", ha="left", alpha=0.9)

    # Annotate extremes on temp only (cleaner).
    if t is not None and t.notna().any():
        i_hi = t.idxmax()
        i_lo = t.idxmin()
        _annotate_extreme(ax, valid.loc[i_hi], t.loc[i_hi],
                          label=f"{t.loc[i_hi]:.0f}°", color=C_TEMP, above=True)
        _annotate_extreme(ax, valid.loc[i_lo], t.loc[i_lo],
                          label=f"{t.loc[i_lo]:.0f}°", color=C_TEMP, above=False)

    ax.set_ylabel("°F")
    ax.legend(loc="upper left", ncol=3, handlelength=1.6)
    ax.grid(True, axis="y")
    _auto_time_axis(ax, valid)


def _panel_pressure(ax, df: pd.DataFrame) -> None:
    ax.set_title("STATION PRESSURE", loc="left")
    valid = df["valid"]
    if "pres1" not in df or df["pres1"].notna().sum() == 0:
        ax.text(0.5, 0.5, "no pressure data", transform=ax.transAxes,
                ha="center", va="center", color=MUTED)
        ax.set_axis_off()
        return

    p = df["pres1"]
    # Thin fill beneath for volume.
    ax.fill_between(valid, p.min(), p, color=C_PRES, alpha=0.12, linewidth=0)
    ax.plot(valid, p, color=C_PRES, lw=1.1)

    # Extremes.
    i_lo = p.idxmin()
    i_hi = p.idxmax()
    if pd.notna(i_hi):
        _annotate_extreme(ax, valid.loc[i_hi], p.loc[i_hi],
                          label=f"{p.loc[i_hi]:.2f}", color=C_PRES, above=True)
    if pd.notna(i_lo):
        _annotate_extreme(ax, valid.loc[i_lo], p.loc[i_lo],
                          label=f"{p.loc[i_lo]:.2f}", color=C_PRES, above=False)

    # Net trend arrow in the top-right corner.
    valid_pres = p.dropna()
    if len(valid_pres) >= 2:
        delta = valid_pres.iloc[-1] - valid_pres.iloc[0]
        arrow = "↑" if delta > 0.02 else ("↓" if delta < -0.02 else "→")
        color = "#4ade80" if delta > 0.02 else ("#f87171" if delta < -0.02 else MUTED)
        ax.text(0.985, 0.90, f"{arrow} {delta:+.2f}", transform=ax.transAxes,
                fontsize=10, fontweight="bold", color=color,
                ha="right", va="center")
        ax.text(0.985, 0.76, "NET Δ (inHg)", transform=ax.transAxes,
                fontsize=7, color=MUTED, ha="right", va="center")

    ax.set_ylabel("inHg")
    ax.grid(True, axis="y")
    _auto_time_axis(ax, valid)


def _panel_wind_rose(ax, df: pd.DataFrame) -> None:
    """Circular histogram of wind direction stacked by speed bin."""
    ax.set_title("WIND ROSE", pad=18, loc="center")
    ax.set_facecolor(BG_PANEL)

    d = df[["drct", "sknt"]].dropna()
    d = d[(d["sknt"] > 0) & (d["drct"] >= 0)]
    if d.empty:
        ax.text(0.5, 0.5, "no wind data", transform=ax.transAxes,
                ha="center", va="center", color=MUTED)
        return

    dir_bins = np.arange(0, 361, 22.5)
    speed_bins = [0, 5, 10, 15, 20, 30, 100]
    speed_colors = ["#0ea5e9", "#22d3ee", "#4ade80", "#facc15",
                    "#f97316", "#ef4444"]
    speed_labels = ["0–5", "5–10", "10–15", "15–20", "20–30", ">30 kt"]

    theta = np.deg2rad(0.5 * (dir_bins[:-1] + dir_bins[1:]))
    width = np.deg2rad(22.5)
    bottom = np.zeros(len(theta))

    total = len(d)
    for (lo, hi), color, label in zip(zip(speed_bins[:-1], speed_bins[1:]),
                                      speed_colors, speed_labels):
        mask = (d["sknt"] >= lo) & (d["sknt"] < hi)
        counts, _ = np.histogram(d.loc[mask, "drct"] % 360, bins=dir_bins)
        pct = counts / max(total, 1) * 100
        ax.bar(theta, pct, width=width, bottom=bottom,
               color=color, edgecolor=BG_PANEL, linewidth=0.6,
               label=label, zorder=3)
        bottom = bottom + pct

    # Radial percent rings (labels at N/NE).
    rmax = bottom.max()
    ring_step = max(1, int(np.ceil(rmax / 5)))
    rings = np.arange(ring_step, rmax + ring_step, ring_step)
    ax.set_rticks(rings)
    ax.set_yticklabels([f"{int(r)}%" for r in rings],
                       fontsize=7, color=MUTED)
    ax.tick_params(axis="y", pad=2)

    # Cardinal labels.
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_xticks(np.deg2rad(np.arange(0, 360, 45)))
    ax.set_xticklabels(["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
                       color=FG, fontweight="bold", fontsize=9)
    ax.tick_params(axis="x", pad=6)
    ax.spines["polar"].set_color(BORDER)
    ax.grid(True, color=GRID, alpha=0.6, linewidth=0.6)

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.22),
              ncol=3, title="SPEED  (knots)", title_fontsize=7.2,
              fontsize=7.2, labelcolor=SOFT, columnspacing=1.0,
              handlelength=1.2, handleheight=0.8)


def _panel_wind(ax, df: pd.DataFrame) -> None:
    ax.set_title("WIND SPEED · GUSTS", loc="left")
    valid = df["valid"]
    if "sknt" in df:
        ax.fill_between(valid, 0, df["sknt"], color=C_WIND,
                        alpha=0.22, linewidth=0, zorder=1)
        ax.plot(valid, df["sknt"], color=C_WIND, lw=1.0,
                label="Wind (2-min mean)", zorder=3)
    if "gust_sknt" in df:
        ax.plot(valid, df["gust_sknt"], color=C_GUST, lw=0.7,
                alpha=0.95, label="Gust (1-min peak)", zorder=2)
        # Annotate peak gust.
        gs = df["gust_sknt"]
        if gs.notna().any():
            idx = gs.idxmax()
            _annotate_extreme(ax, valid.loc[idx], gs.loc[idx],
                              label=f"{gs.loc[idx]:.0f} kt",
                              color=C_GUST, above=True)
    ax.set_ylabel("knots")
    ax.legend(loc="upper left", ncol=2, handlelength=1.6)
    ax.grid(True, axis="y")
    _auto_time_axis(ax, valid)


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

    # Per-interval bars.
    ax.bar(series.index, series.values, width=width_days, align="edge",
           color=C_PRECIP, edgecolor="none", alpha=0.9, label=unit)
    ax.set_ylabel(unit, color=C_PRECIP)
    ax.tick_params(axis="y", colors=C_PRECIP)
    ax.grid(True, axis="y")

    # Cumulative overlay on twin axis.
    cumulative = series.fillna(0).cumsum()
    ax2 = ax.twinx()
    ax2.plot(cumulative.index, cumulative.values, color=C_CUM, lw=1.6,
             label="Cumulative total", zorder=5)
    ax2.fill_between(cumulative.index, 0, cumulative.values,
                     color=C_CUM, alpha=0.10, linewidth=0)
    ax2.set_ylabel("cumulative (in)", color=C_CUM)
    ax2.tick_params(axis="y", colors=C_CUM)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color(BORDER)
    ax2.grid(False)

    # Unified legend.
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=C_PRECIP, alpha=0.9),
        plt.Line2D([0], [0], color=C_CUM, lw=1.6),
    ]
    ax.legend(handles, [unit, "cumulative"], loc="upper left",
              ncol=2, handlelength=1.6)

    total = float(df["precip"].sum(skipna=True))
    # Top-right callout with total precip.
    ax.text(0.985, 0.90, f"{total:.2f} in", transform=ax.transAxes,
            fontsize=13, fontweight="bold", color=FG_HI,
            ha="right", va="center")
    ax.text(0.985, 0.76, "TOTAL ACCUM.", transform=ax.transAxes,
            fontsize=7, color=MUTED, ha="right", va="center")

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

    fig = plt.figure(figsize=(14, 10), dpi=120)
    gs = GridSpec(
        5, 4,
        height_ratios=[1.15, 1.6, 1.25, 1.25, 1.35],
        hspace=0.55, wspace=0.35,
        left=0.055, right=0.97, top=0.96, bottom=0.06,
    )

    # Row 0 — header strip spans all columns.
    ax_head = fig.add_subplot(gs[0, :])
    _draw_header(fig, ax_head, station_id=station_id, station_name=name,
                 window_label=window_label, df=df)

    # Rows 1–2, cols 0–2 — temperature + pressure.
    ax_temp = fig.add_subplot(gs[1, 0:3])
    ax_pres = fig.add_subplot(gs[2, 0:3], sharex=ax_temp)
    _panel_temp_dew(ax_temp, df)
    _panel_pressure(ax_pres, df)
    # hide shared x-axis tick labels on the upper panel
    plt.setp(ax_temp.get_xticklabels(), visible=False)

    # Rows 1–2, col 3 — wind rose (polar) spanning both rows.
    ax_rose = fig.add_subplot(gs[1:3, 3], projection="polar")
    _panel_wind_rose(ax_rose, df)

    # Row 3 full — wind speed/gust.
    ax_wind = fig.add_subplot(gs[3, :])
    _panel_wind(ax_wind, df)

    # Row 4 full — precipitation.
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

    chips = [
        ("FLAG RATE", f"{rate:.1f}%", C_FLAG),
        ("FLAGGED METARS", f"{n_flagged:,}", C_FLAG),
        ("CLEAN METARS", f"{n_total - n_flagged:,}", C_CLEAN),
        ("STATIONS", f"{n_stations}", ACCENT),
    ]
    n = len(chips)
    chip_y, chip_h, gap = 0.04, 0.38, 0.012
    chip_w = (1.0 - gap * (n - 1)) / n
    for i, (label, value, accent) in enumerate(chips):
        x_left = i * (chip_w + gap)
        chip = FancyBboxPatch((x_left, chip_y), chip_w, chip_h,
                              transform=ax.transAxes,
                              boxstyle="round,pad=0.0,rounding_size=0.04",
                              facecolor=BG_CHIP, edgecolor=BORDER,
                              linewidth=0.9, clip_on=False)
        ax.add_patch(chip)
        stripe = Rectangle((x_left, chip_y + 0.04), 0.005, chip_h - 0.08,
                           transform=ax.transAxes, color=accent, clip_on=False)
        ax.add_patch(stripe)
        ax.text(x_left + 0.020, chip_y + chip_h - 0.06, label,
                transform=ax.transAxes, fontsize=7.4, color=MUTED,
                ha="left", va="top")
        ax.text(x_left + 0.020, chip_y + 0.07, value,
                transform=ax.transAxes, fontsize=16, fontweight="bold",
                color=FG_HI, ha="left", va="bottom")


def _panel_flag_rate_per_station(ax, metars_df: pd.DataFrame) -> None:
    ax.set_title("FLAG RATE  BY  STATION", loc="left")
    per_stn = metars_df.groupby("station")["has_maintenance"].agg(["mean", "count"])
    per_stn["rate"] = per_stn["mean"] * 100
    per_stn = per_stn.sort_values("rate")

    colors = [C_CLEAN if r < 10 else ("#fbbf24" if r < 50 else C_FLAG)
              for r in per_stn["rate"]]
    bars = ax.barh(per_stn.index, per_stn["rate"],
                   color=colors, edgecolor="none", height=0.72)
    ax.set_xlim(0, 105)
    ax.set_xlabel("% of METARs ending in $")
    ax.grid(True, axis="x", alpha=0.4)
    for bar, rate, count in zip(bars, per_stn["rate"], per_stn["count"]):
        ax.text(rate + 1, bar.get_y() + bar.get_height() / 2,
                f"{rate:.0f}%  ({count})", va="center", color=SOFT, fontsize=8)


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

    chips = [
        ("CLEAN $ FREE",  f"{n_clean:,}",      C_CLEAN),
        ("FLAGGED  $",    f"{n_flag:,}",       C_FLAG),
        ("FLAG RATE",     f"{rate:.1f}%",      C_FLAG),
        ("DELTA RATIO",   f"{(n_flag/max(n_clean,1)):.2f}x",  ACCENT),
    ]
    n = len(chips)
    chip_y, chip_h, gap = 0.04, 0.38, 0.012
    chip_w = (1.0 - gap * (n - 1)) / n
    for i, (label, value, accent) in enumerate(chips):
        x_left = i * (chip_w + gap)
        ax.add_patch(FancyBboxPatch((x_left, chip_y), chip_w, chip_h,
                                    transform=ax.transAxes,
                                    boxstyle="round,pad=0.0,rounding_size=0.04",
                                    facecolor=BG_CHIP, edgecolor=BORDER,
                                    linewidth=0.9, clip_on=False))
        ax.add_patch(Rectangle((x_left, chip_y + 0.04), 0.005, chip_h - 0.08,
                               transform=ax.transAxes,
                               color=accent, clip_on=False))
        ax.text(x_left + 0.020, chip_y + chip_h - 0.06, label,
                transform=ax.transAxes, fontsize=7.4, color=MUTED,
                ha="left", va="top")
        ax.text(x_left + 0.020, chip_y + 0.07, value,
                transform=ax.transAxes, fontsize=16, fontweight="bold",
                color=FG_HI, ha="left", va="bottom")


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
        "•  A trailing $ on a METAR is the ASOS maintenance check indicator; "
        "treat the associated sensor readings with extra skepticism.",
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
