"""ASOS Tools — operational dashboard for the federal ASOS network."""

from __future__ import annotations

import io
import logging
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import requests as _requests
import streamlit as st

from asos_tools import fetch_1min, fetch_metars
from asos_tools.report import (
    build_comparison_report,
    build_maintenance_report,
    build_report,
)
from asos_tools._missing_report import build_missing_report
from asos_tools.nws import get_current_conditions
from asos_tools.stations import GROUPS, get_group, list_groups
from asos_tools.watchlist import build_watchlist

logger = logging.getLogger(__name__)

try:
    from asos_tools.stations import ALL_ASOS_STATIONS
    _HAVE_CATALOG = bool(ALL_ASOS_STATIONS)
except ImportError:
    ALL_ASOS_STATIONS = []
    _HAVE_CATALOG = False

try:
    from asos_tools.stations import AOMC_STATIONS, AOMC_IDS
    _HAVE_AOMC = bool(AOMC_STATIONS)
except ImportError:
    AOMC_STATIONS = []
    AOMC_IDS = frozenset()
    _HAVE_AOMC = False

# Pre-computed lookups (perf fix: avoid O(n) scans and per-call dict builds).
_AOMC_META = {s["id"]: s for s in AOMC_STATIONS}
_SCAN_HOURS = 4
_MAX_CUSTOM_DAYS = 365


def _round_3min(dt: datetime) -> str:
    """Round to nearest 3-min boundary — matches scan cache TTL."""
    return dt.replace(second=0, microsecond=0,
                      minute=(dt.minute // 3) * 3).isoformat()


def _round_5min(dt: datetime) -> datetime:
    """Round to nearest 5-min boundary for stable fetch cache keys."""
    return dt.replace(second=0, microsecond=0,
                      minute=(dt.minute // 5) * 5)


def _wlabel(days: int) -> str:
    return f"{days} day{'s' if days != 1 else ''}"


# ---------------------------------------------------------------------------
# Cached fetches (perf fixes applied)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def _fetch_1min(station, start, end):
    return fetch_1min(station, start, end)

@st.cache_data(ttl=600, show_spinner=False)
def _fetch_metars(stations_key, start, end):
    return fetch_metars(list(stations_key), start, end)

@st.cache_data(ttl=120, show_spinner=False)
def _fetch_nws(station_id):
    # Security: validate station ID before passing to NWS URL path.
    if not re.fullmatch(r"[A-Z0-9]{3,6}", station_id.strip().upper()):
        return None
    return get_current_conditions(station_id)

@st.cache_data(ttl=180, show_spinner=False)
def _scan(station_ids, hours, cache_key):
    """Watchlist scan. cache_key is rounded to 3-min to match TTL."""
    end = datetime.fromisoformat(cache_key).replace(tzinfo=timezone.utc)
    return build_watchlist(
        [_AOMC_META.get(sid, {"id": sid}) for sid in station_ids],
        hours=hours, end=end,
    )


def _render(builder, **kw):
    """Render a report PNG via a temp file (matplotlib needs a path)."""
    with NamedTemporaryFile(suffix=".png", delete=False) as f:
        tmp = Path(f.name)
    try:
        builder(out_path=tmp, **kw)
        return tmp.read_bytes()
    finally:
        tmp.unlink(missing_ok=True)


def _short_name(name: str) -> str:
    """Title-case a station name and trim common suffixes."""
    n = (name or "").strip().title()
    for suf in [" Intl Ap", " Intl Airport", " Rgnl Ap", " Muni Ap",
                " Ap", " Airport", " Arpt"]:
        if n.endswith(suf):
            n = n[:-len(suf)]
            break
    return n if len(n) <= 28 else n[:26] + "…"


# ---------------------------------------------------------------------------
# Page config + minimal CSS
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="O.W.L. — Observation Watch Log",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""<style>
/* Professional typography: Inter for UI, Space Grotesk for headings */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="st-"], [class*="css-"], button, input, select, textarea {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif !important;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    font-feature-settings: 'cv11', 'ss01', 'ss03';
}

h1, h2, h3, h4, h5, h6,
[data-testid="stHeading"] {
    font-family: 'Space Grotesk', 'Inter', sans-serif !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em;
}

code, pre, [data-testid="stCode"] {
    font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace !important;
}

/* Layout */
section.main > div.block-container {
    padding-top: 1.4rem; max-width: 1460px;
}
[data-testid="stSidebar"] { min-width: 320px; max-width: 380px; }
[data-testid="stSidebar"] > div {
    padding-top: 0;
    padding-left: 0 !important;
    padding-right: 0 !important;
}

/* === SIDEBAR LOGO — seamless, with breathing room === */
[data-testid="stSidebar"] [data-testid="stImage"],
[data-testid="stSidebar"] [data-testid="stImage"]:first-of-type {
    border: none !important;
    border-radius: 0 !important;
    background: transparent !important;
    box-shadow: none !important;
    padding: 0.5rem 0 0 0 !important;
    margin: 0 0 0.5rem 0 !important;
    min-height: 0 !important;
}
[data-testid="stSidebar"] [data-testid="stImage"] img {
    display: block;
    margin: 0 auto;
    max-width: 82% !important;
    height: auto !important;
    filter: drop-shadow(0 2px 8px rgba(15, 23, 42, 0.12));
}

/* Sidebar content padding (applied after image) */
[data-testid="stSidebar"] > div > div {
    padding-left: 1rem !important;
    padding-right: 1rem !important;
}

/* === LIGHT MODE POLISH === */

/* Sidebar has subtle off-white tint to visually separate from main. */
[data-testid="stSidebar"] {
    background: #f8fafc !important;
    border-right: 1px solid #e2e8f0 !important;
}

/* Metric cards — NOAA blue accent + visible border */
div[data-testid="stMetric"] {
    padding: 0.7rem 0.9rem;
    border-radius: 10px;
    border: 1px solid #e2e8f0;
    border-left: 3px solid #003366;
    background: #ffffff;
    box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}
div[data-testid="stMetric"] label {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    opacity: 0.72;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-family: 'Space Grotesk', 'Inter', sans-serif !important;
    font-size: 1.45rem !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em;
    line-height: 1.15;
}

/* Tabs — clean underline style */
.stTabs [data-baseweb="tab-list"] { gap: 0; }
.stTabs [data-baseweb="tab"] {
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    padding: 0.65rem 1.15rem !important;
    border-radius: 0 !important;
    border-bottom: 2px solid transparent !important;
    letter-spacing: -0.005em;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    border-bottom-color: #003366 !important;
    color: #003366 !important;
    font-weight: 700 !important;
}
.stTabs [data-baseweb="tab"]:hover {
    border-bottom-color: rgba(128,128,128,0.3) !important;
}

/* Tables */
[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }

/* Sidebar section headers — refined hierarchy */
[data-testid="stSidebar"] h3 {
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.14em !important;
    text-transform: uppercase;
    opacity: 0.55;
    margin-top: 1rem !important;
    margin-bottom: 0.4rem !important;
}
[data-testid="stSidebar"] h4 {
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    opacity: 0.8;
    margin-top: 0.8rem !important;
    margin-bottom: 0.3rem !important;
}

/* Sidebar captions — tighter line-height */
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] p {
    font-size: 0.78rem !important;
    line-height: 1.4;
}

/* Brand line in sidebar — under logo */
.owl-brand {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.28em;
    text-transform: uppercase;
    text-align: center;
    color: #64748b;
    margin: -0.5rem 0 1rem 0;
    padding-bottom: 0.8rem;
    border-bottom: 1px solid #e2e8f0;
}

/* Main page title */
h2 {
    font-size: 1.9rem !important;
    letter-spacing: -0.025em;
    margin-bottom: 0.2rem !important;
}

/* Buttons */
[data-testid="stDownloadButton"] > button,
.stButton > button {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.85rem !important;
    font-weight: 600 !important;
    letter-spacing: -0.005em;
    border-radius: 8px !important;
}
.stButton > button[kind="primary"] {
    background-color: #003366 !important;
    color: #ffffff !important;
    font-weight: 700 !important;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    font-size: 0.82rem !important;
    border: none !important;
    box-shadow: 0 2px 6px rgba(0, 51, 102, 0.25);
}
.stButton > button[kind="primary"]:hover {
    background-color: #1e40af !important;
    box-shadow: 0 4px 12px rgba(0, 51, 102, 0.35);
}

/* Dividers — thinner */
hr {
    border-color: #e2e8f0 !important;
    margin: 0.8rem 0 !important;
}

/* Main content image (reports) — clean border */
.main [data-testid="stImage"] {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid rgba(128,128,128,0.12);
}

/* === OPERATIONS BANNER (top of page) === */
.ops-banner {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 1rem;
    margin: -0.6rem -0.2rem 0.9rem -0.2rem;
    background: #1a1f2e;
    color: #cbd5e1;
    border-radius: 6px;
    font-family: 'Inter', sans-serif;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    box-shadow: 0 2px 6px rgba(0,0,0,0.06);
}
.ops-banner-left, .ops-banner-right {
    display: flex; align-items: center; gap: 0.7rem;
}
.banner-pill {
    background: #00a91c;
    color: #ffffff;
    padding: 0.15rem 0.55rem;
    border-radius: 4px;
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.12em;
}
.banner-meta {
    color: #94a3b8;
    text-transform: uppercase;
}
.banner-time {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    color: #cbd5e1;
    letter-spacing: 0.02em;
}
.status-dot {
    width: 9px; height: 9px;
    border-radius: 50%;
    display: inline-block;
    animation: pulse 2.4s ease-in-out infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}
.status-label {
    font-weight: 700;
    letter-spacing: 0.1em;
    font-size: 0.74rem;
}

/* === TITLE BLOCK === */
.ops-title {
    border-bottom: 2px solid #003366;
    padding-bottom: 0.7rem;
    margin-bottom: 1.1rem;
}
.ops-title-name {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 2.4rem;
    font-weight: 700;
    letter-spacing: -0.04em;
    color: #003366;
    line-height: 1;
}
.ops-title-sub {
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    font-weight: 500;
    color: #64748b;
    margin-top: 0.25rem;
    letter-spacing: 0.01em;
}

/* === NETWORK HEALTH GAUGE === */
.health-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1rem 1.2rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04);
}
.health-row {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    margin-bottom: 0.7rem;
}
.health-label {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.18em;
    color: #64748b;
    text-transform: uppercase;
}
.health-numbers {
    display: flex;
    align-items: baseline;
    gap: 0.7rem;
    margin-top: 0.2rem;
}
.health-pct {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: -0.03em;
    color: #003366;
    line-height: 1;
}
.health-detail {
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    color: #64748b;
}
.health-stats {
    display: flex;
    gap: 1rem;
    align-items: flex-end;
}
.health-stat {
    font-family: 'Inter', sans-serif;
    font-size: 0.78rem;
    font-weight: 600;
    color: #475569;
    display: flex;
    align-items: center;
    gap: 0.4rem;
    letter-spacing: 0.04em;
}
.health-stat .dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    display: inline-block;
}
.health-bar-track {
    width: 100%;
    height: 8px;
    background: #f1f5f9;
    border-radius: 4px;
    overflow: hidden;
}
.health-bar-fill {
    height: 100%;
    border-radius: 4px;
    transition: width 0.6s ease;
}

/* === FEDERAL FOOTER === */
.fed-footer {
    margin-top: 2rem;
    padding: 1rem 1.2rem;
    background: #f8fafc;
    border-top: 3px solid #003366;
    border-radius: 0 0 8px 8px;
    font-family: 'Inter', sans-serif;
}
.fed-footer-cite {
    font-size: 0.74rem;
    color: #475569;
    line-height: 1.55;
}
.fed-footer-cite strong { color: #003366; }
.fed-footer-meta {
    margin-top: 0.6rem;
    padding-top: 0.6rem;
    border-top: 1px solid #e2e8f0;
    font-size: 0.7rem;
    color: #94a3b8;
    letter-spacing: 0.04em;
}
.fed-footer-meta a {
    color: #003366;
    text-decoration: none;
    font-weight: 600;
}

/* Print styles for government printouts */
@media print {
    [data-testid="stSidebar"], .ops-banner, .stTabs [data-baseweb="tab-list"] { display: none !important; }
    body { background: #ffffff !important; color: #000000 !important; }
}
</style>""", unsafe_allow_html=True)


# ===========================================================================
# SIDEBAR — always visible, independent of active tab
# ===========================================================================

with st.sidebar:
    logo = Path(__file__).parent / "owl_logo.png"
    if logo.exists():
        st.image(str(logo), use_container_width=True)
    st.markdown(
        '<div class="owl-brand">Observation Watch Log</div>',
        unsafe_allow_html=True,
    )
    st.caption(f"{len(AOMC_STATIONS)} ASOS stations · NWS / FAA / DOD")

    # ---- Quick network pulse (cached at 3-min boundary) ----
    if _HAVE_AOMC:
        all_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
        ck = _round_3min(datetime.now(timezone.utc))
        try:
            pulse = _scan(all_ids, 4.0, ck)
            if not pulse.empty:
                cts = pulse["status"].value_counts()
                pc1, pc2, pc3 = st.columns(3)
                pc1.metric("Clean", int(cts.get("CLEAN", 0)))
                pc2.metric("Flagged", int(cts.get("FLAGGED", 0)))
                pc3.metric("Missing",
                           int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0)))
                st.caption(f"Last 4h · {datetime.now(timezone.utc):%H:%M UTC}")
        except Exception as exc:
            logger.exception("Sidebar pulse scan failed")
            st.warning("Network scan unavailable")

    st.divider()

    # ---- Report controls ----
    st.markdown("#### Report Settings")

    report_type = st.selectbox(
        "Report type",
        ["1-min dashboard", "Maintenance ($)", "Flagged vs clean", "Missing METARs"],
    )

    smode = st.radio("Station source", ["Single", "Group", "Custom"],
                     horizontal=True, label_visibility="collapsed")

    stations_list: list[str] = []
    group_label = ""

    if smode == "Single":
        aomc_toggle = st.toggle("AOMC stations only", value=True) if _HAVE_AOMC else False
        if _HAVE_CATALOG:
            pool = ([s for s in ALL_ASOS_STATIONS if s["id"] in AOMC_IDS]
                    if aomc_toggle else ALL_ASOS_STATIONS)
            pool_ids = [s["id"] for s in pool]
            _plk = {s["id"]: s for s in pool}  # O(1) lookup (was O(n²))
            sid = st.selectbox(
                "Station", pool_ids,
                index=pool_ids.index("KJFK") if "KJFK" in pool_ids else 0,
                format_func=lambda s: (
                    f"{s} · {_short_name(_plk.get(s, {}).get('name', ''))} · "
                    f"{_plk.get(s, {}).get('state', '')}"
                ),
                help="Select by ICAO ID, name, or state",
            )
        else:
            sid = st.text_input("ICAO ID", "KJFK").strip().upper()
        stations_list = [sid]
        group_label = sid
    elif smode == "Group":
        grp = st.selectbox("Preset group", list_groups(),
                           format_func=lambda s: s.replace("_", " ").title())
        stations_list = list(get_group(grp))
        group_label = grp.replace("_", " ").title()
        st.caption(f"{len(stations_list)} stations")
    else:
        raw = st.text_area("Station IDs (comma or newline)", "KJFK, KLGA, KEWR",
                           height=68)
        stations_list = [s.strip().upper() for s in raw.replace(",", "\n").splitlines() if s.strip()]
        group_label = ", ".join(stations_list[:3]) + ("…" if len(stations_list) > 3 else "")
        st.caption(f"{len(stations_list)} stations")

    wmode = st.selectbox(
        "Time window",
        ["1 day", "7 days", "14 days", "30 days", "Custom"],
        index=1,
    )
    if wmode == "Custom":
        today = datetime.now(timezone.utc).date()
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("From", today - timedelta(days=7))
        with c2:
            ed = st.date_input("To", today)
        start = datetime.combine(sd, datetime.min.time(), tzinfo=timezone.utc)
        end = datetime.combine(ed, datetime.min.time(), tzinfo=timezone.utc)
        span = (end - start).days
        if span > _MAX_CUSTOM_DAYS:
            st.error(f"Date range too large ({span} days). Maximum is {_MAX_CUSTOM_DAYS} days.")
        elif span <= 0:
            st.error("End date must be after start date.")
        wlabel = _wlabel(span)
    else:
        days = {"1 day": 1, "7 days": 7, "14 days": 14, "30 days": 30}[wmode]
        end = _round_5min(datetime.now(timezone.utc))
        start = _round_5min(end - timedelta(days=days))
        wlabel = _wlabel(days)

    go = st.button("Generate", type="primary", use_container_width=True)

    # ---- Live weather for single station ----
    if smode == "Single" and stations_list:
        st.divider()
        cond = _fetch_nws(stations_list[0])
        if cond:
            st.markdown(f"#### {stations_list[0]} Now")
            st.caption(cond.get("description", ""))
            wc1, wc2 = st.columns(2)
            wc1.metric("Temp",
                       f"{cond['temp_f']:.0f}°F" if cond.get("temp_f") is not None else "—")
            wc2.metric("Dew",
                       f"{cond['dewpoint_f']:.0f}°F" if cond.get("dewpoint_f") is not None else "—")
            wc1, wc2 = st.columns(2)
            w_spd = cond.get("wind_speed_kt")
            w_dir = cond.get("wind_direction")
            wc1.metric("Wind",
                       f"{w_dir:.0f}° / {w_spd:.0f} kt" if w_spd is not None else "—")
            wc2.metric("Vis",
                       f"{cond['visibility_mi']:.0f}mi" if cond.get("visibility_mi") is not None else "—")
            sky = cond.get("sky", "")
            if sky and sky != "CLR":
                st.caption(f"Sky: {sky}")
            ts = cond.get("timestamp", "")
            if ts:
                st.caption(f"Obs: {ts[:16]}Z")
        else:
            st.caption(f"NWS data unavailable for {stations_list[0]}")

    # ---- Sidebar footer ----
    st.divider()
    fc1, fc2 = st.columns(2)
    with fc1:
        if st.button("Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with fc2:
        # Dark mode toggle — Streamlit handles the rest via
        # its built-in theme system. We just re-render.
        if "dark_mode" not in st.session_state:
            st.session_state.dark_mode = False
        if st.button("Dark" if not st.session_state.dark_mode else "Light",
                     use_container_width=True, key="theme_btn"):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()

    # Apply dark overrides if toggled on.
    if st.session_state.get("dark_mode"):
        st.markdown("""<style>
        /* === DARK MODE — deep navy with distinct layers === */
        /* Main: darkest. Sidebar: mid. Cards: slightly lighter. */
        [data-testid="stApp"],
        [data-testid="stAppViewContainer"],
        .main { background-color: #0a1020 !important; color: #e2e8f0 !important; }
        [data-testid="stHeader"] { background-color: #0a1020 !important; }

        [data-testid="stSidebar"] {
            background-color: #111a2e !important;
            border-right: 1px solid #1f2a44 !important;
        }
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
            color: #cbd5e1 !important;
        }
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        [data-testid="stSidebar"] h4 {
            color: #f1f5f9 !important;
        }
        [data-testid="stSidebar"] .owl-brand {
            color: #94a3b8 !important;
            border-bottom-color: #1f2a44 !important;
        }

        h1, h2, h3, h4 { color: #f1f5f9 !important; }
        p, [data-testid="stMarkdownContainer"] { color: #cbd5e1 !important; }
        [data-testid="stCaptionContainer"] { color: #94a3b8 !important; }

        /* Metric cards — distinct elevation from sidebar bg */
        div[data-testid="stMetric"] {
            background: #1a2540 !important;
            border: 1px solid #2d3a5c !important;
            border-left: 3px solid #38bdf8 !important;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3) !important;
        }
        div[data-testid="stMetric"] label { color: #94a3b8 !important; }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #f1f5f9 !important;
        }

        /* Logo drop-shadow — softer on dark */
        [data-testid="stSidebar"] [data-testid="stImage"] img {
            filter: drop-shadow(0 4px 16px rgba(56, 189, 248, 0.25)) !important;
        }

        /* Tables */
        [data-testid="stDataFrame"] {
            border-color: #2d3a5c !important;
            background: #111a2e !important;
        }

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            border-bottom: 1px solid #1f2a44 !important;
        }
        .stTabs [data-baseweb="tab"] { color: #94a3b8 !important; }
        .stTabs [data-baseweb="tab"]:hover { color: #cbd5e1 !important; }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            color: #f1f5f9 !important;
            border-bottom-color: #38bdf8 !important;
        }

        /* Images */
        .main [data-testid="stImage"] {
            background: #111a2e !important;
            border-color: #1f2a44 !important;
        }

        /* EVERY form control — nuclear approach to catch all Streamlit variants */
        [data-baseweb="select"],
        [data-baseweb="select"] > div,
        [data-baseweb="select"] > div > div,
        [data-baseweb="select"] [class*="control"],
        [data-baseweb="select"] [class*="ValueContainer"],
        [data-baseweb="select"] [class*="singleValue"],
        [data-baseweb="select"] [class*="placeholder"],
        [data-baseweb="select"] [class*="indicatorContainer"],
        div[data-baseweb="select"] div[role="combobox"],
        div[data-baseweb="select"] div[role="listbox"] {
            background-color: #1e293b !important;
            color: #f1f5f9 !important;
            border-color: #475569 !important;
        }
        [data-baseweb="select"] span,
        [data-baseweb="select"] div,
        [data-baseweb="select"] input {
            color: #f1f5f9 !important;
        }
        [data-baseweb="select"] svg { fill: #94a3b8 !important; }
        [data-baseweb="select"] [data-baseweb="tag"] { background: #334155 !important; color: #f1f5f9 !important; }

        /* Dropdown menus / popovers — every depth */
        [data-baseweb="popover"],
        [data-baseweb="popover"] > div,
        [data-baseweb="menu"],
        [data-baseweb="menu"] > div,
        ul[role="listbox"],
        ul[role="listbox"] > li,
        div[role="listbox"],
        div[role="listbox"] > div {
            background-color: #1e293b !important;
            color: #e2e8f0 !important;
        }
        ul[role="listbox"] > li:hover,
        [data-baseweb="menu"] li:hover,
        [data-baseweb="popover"] li:hover,
        div[role="option"]:hover,
        li[role="option"]:hover {
            background-color: #334155 !important;
        }
        li[role="option"],
        div[role="option"] { color: #e2e8f0 !important; }
        li[aria-selected="true"],
        div[aria-selected="true"] { background-color: #0f172a !important; }

        /* Text inputs */
        [data-baseweb="input"],
        [data-baseweb="input"] > div {
            background-color: #1e293b !important;
            border-color: #475569 !important;
        }
        [data-baseweb="input"] input {
            color: #f1f5f9 !important;
            background: transparent !important;
            -webkit-text-fill-color: #f1f5f9 !important;
        }
        [data-baseweb="textarea"] textarea {
            color: #f1f5f9 !important;
            background: #1e293b !important;
            border-color: #475569 !important;
            -webkit-text-fill-color: #f1f5f9 !important;
        }

        /* Radio + checkbox + toggle */
        [data-baseweb="radio"] label,
        [data-baseweb="checkbox"] label { color: #e2e8f0 !important; }
        [data-testid="stWidgetLabel"] { color: #cbd5e1 !important; }

        /* Captions */
        [data-testid="stCaptionContainer"] { color: #94a3b8 !important; }

        /* ALL buttons — every variant Streamlit renders */
        .stButton > button,
        .stButton > button:focus,
        .stButton > button:active,
        [data-testid="stBaseButton-secondary"],
        [data-testid="stBaseButton-secondary"]:focus,
        button[kind="secondary"],
        button[kind="secondary"]:focus {
            background-color: #1e293b !important;
            color: #f1f5f9 !important;
            border: 1px solid #475569 !important;
            -webkit-text-fill-color: #f1f5f9 !important;
        }
        .stButton > button:hover,
        [data-testid="stBaseButton-secondary"]:hover,
        button[kind="secondary"]:hover {
            background-color: #334155 !important;
            border-color: #64748b !important;
        }
        /* Primary button — NOAA blue, light-blue text for contrast */
        .stButton > button[kind="primary"],
        [data-testid="stBaseButton-primary"] {
            background-color: #1e40af !important;
            color: #f0f9ff !important;
            -webkit-text-fill-color: #f0f9ff !important;
            border: none !important;
            box-shadow: 0 2px 8px rgba(30, 64, 175, 0.35) !important;
        }
        .stButton > button[kind="primary"]:hover,
        [data-testid="stBaseButton-primary"]:hover {
            background-color: #2563eb !important;
        }
        /* Download buttons */
        [data-testid="stDownloadButton"] > button,
        [data-testid="stDownloadButton"] > button:focus {
            color: #e2e8f0 !important;
            -webkit-text-fill-color: #e2e8f0 !important;
            background-color: #1e293b !important;
            border: 1px solid #475569 !important;
        }
        [data-testid="stDownloadButton"] > button:hover {
            background-color: #334155 !important;
        }
        /* Button text — catch any nested span/p */
        .stButton > button span,
        .stButton > button p,
        [data-testid="stDownloadButton"] > button span,
        [data-testid="stDownloadButton"] > button p {
            color: inherit !important;
            -webkit-text-fill-color: inherit !important;
        }

        /* Expanders */
        [data-testid="stExpander"] summary { color: #e2e8f0 !important; }
        [data-testid="stExpander"] { border-color: #334155 !important; }

        /* Alerts */
        [data-testid="stAlert"] { background: #1e293b !important; color: #e2e8f0 !important; }

        /* Dividers */
        hr { border-color: #334155 !important; }

        /* Scrollbar */
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }

        /* Date input */
        [data-baseweb="datepicker"] { background: #1e293b !important; }
        [data-baseweb="datepicker"] * { color: #e2e8f0 !important; }
        [data-baseweb="calendar"] { background: #1e293b !important; }

        /* Ops banner / title / health card — dark mode */
        .ops-banner { background: #0a1020 !important; }
        .ops-title { border-bottom-color: #38bdf8 !important; }
        .ops-title-name { color: #f8fafc !important; }
        .ops-title-sub { color: #94a3b8 !important; }
        .health-card {
            background: #1a2540 !important;
            border-color: #2d3a5c !important;
        }
        .health-label { color: #94a3b8 !important; }
        .health-pct { color: #38bdf8 !important; }
        .health-detail { color: #cbd5e1 !important; }
        .health-stat { color: #cbd5e1 !important; }
        .health-bar-track { background: #1f2a44 !important; }
        .fed-footer {
            background: #111a2e !important;
            border-top-color: #38bdf8 !important;
        }
        .fed-footer-cite { color: #cbd5e1 !important; }
        .fed-footer-cite strong { color: #38bdf8 !important; }
        .fed-footer-meta { color: #94a3b8 !important; border-top-color: #1f2a44 !important; }
        .fed-footer-meta a { color: #38bdf8 !important; }
        </style>""", unsafe_allow_html=True)

    st.caption(
        "[Source](https://github.com/consigcody94/asos-tools-py) · "
        "Data: [NOAA/NCEI](https://www.ncei.noaa.gov) via "
        "[IEM](https://mesonet.agron.iastate.edu)"
    )


# ===========================================================================
# OPERATIONS HEADER — government NOC dashboard aesthetic
# ===========================================================================

# Pre-compute a quick health snapshot for the status banner.
_health_pct = None
_status_label = "OPERATIONAL"
_status_color = "#00a91c"  # USWDS success green
_health_clean = 0
_health_total = 0
_health_problem = 0
if _HAVE_AOMC:
    try:
        _health_ck = _round_3min(datetime.now(timezone.utc))
        _health_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
        _health_wl = _scan(_health_ids, float(_SCAN_HOURS), _health_ck)
        if not _health_wl.empty:
            _hcts = _health_wl["status"].value_counts()
            _health_clean = int(_hcts.get("CLEAN", 0))
            _health_total = len(_health_wl)
            _health_problem = (int(_hcts.get("FLAGGED", 0))
                               + int(_hcts.get("MISSING", 0))
                               + int(_hcts.get("NO DATA", 0)))
            _health_pct = round(_health_clean / _health_total * 100, 1)
            if _health_pct < 70:
                _status_label = "DEGRADED"
                _status_color = "#b50909"  # USWDS error red
            elif _health_pct < 85:
                _status_label = "PARTIAL"
                _status_color = "#ffbe2e"  # USWDS warning yellow
    except Exception:
        _status_label = "UNKNOWN"
        _status_color = "#71767a"  # USWDS gray

# Status banner — sits above everything, like a federal status page.
_now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
st.markdown(f"""
<div class="ops-banner">
  <div class="ops-banner-left">
    <span class="banner-pill">UNCLASSIFIED</span>
    <span class="banner-meta">FOR OFFICIAL USE · ASOS NETWORK MONITORING</span>
  </div>
  <div class="ops-banner-right">
    <span class="status-dot" style="background:{_status_color};box-shadow:0 0 8px {_status_color};"></span>
    <span class="status-label" style="color:{_status_color};">SYSTEM {_status_label}</span>
    <span class="banner-meta banner-time">{_now_iso}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# Title block.
st.markdown("""
<div class="ops-title">
  <div class="ops-title-name">O.W.L.</div>
  <div class="ops-title-sub">Observation Watch Log · National ASOS Operations Dashboard</div>
</div>
""", unsafe_allow_html=True)

# Network health gauge.
if _health_pct is not None and _health_total:
    bar_color = ("#00a91c" if _health_pct >= 85
                 else ("#ffbe2e" if _health_pct >= 70 else "#b50909"))
    st.markdown(f"""
<div class="health-card">
  <div class="health-row">
    <div class="health-meta">
      <div class="health-label">NETWORK HEALTH · LAST {_SCAN_HOURS}H</div>
      <div class="health-numbers">
        <span class="health-pct">{_health_pct:.1f}%</span>
        <span class="health-detail">{_health_clean:,} of {_health_total:,} stations reporting clean</span>
      </div>
    </div>
    <div class="health-stats">
      <div class="health-stat"><span class="dot" style="background:#00a91c;"></span>{_health_clean:,} CLEAN</div>
      <div class="health-stat"><span class="dot" style="background:#b50909;"></span>{_health_problem:,} ATTENTION</div>
    </div>
  </div>
  <div class="health-bar-track">
    <div class="health-bar-fill" style="width:{_health_pct}%;background:{bar_color};"></div>
  </div>
</div>
""", unsafe_allow_html=True)


# ===========================================================================
# Tabs
# ===========================================================================

tab_summary, tab_reports, tab_stations, tab_flags, tab_missing = st.tabs([
    "SUMMARY", "REPORTS", "STATIONS", "$ FLAGS", "MISSING METARS",
])


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _scan_ui(key):
    c1, c2 = st.columns(2)
    with c1:
        hours = st.selectbox("Window", [1, 2, 4, 6, 12, 24], index=2,
                             format_func=lambda h: f"Last {h}h", key=f"{key}_h")
    with c2:
        scope = st.selectbox("Scope", ["All AOMC", "By state", "Preset group"],
                             key=f"{key}_sc")
    if scope == "By state":
        states = sorted({s.get("state") for s in AOMC_STATIONS if s.get("state")})
        pick = st.selectbox("State", states, key=f"{key}_st")
        ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("state") == pick)
    elif scope == "Preset group":
        g = st.selectbox("Group", list_groups(),
                         format_func=lambda s: s.replace("_", " ").title(),
                         key=f"{key}_grp")
        ids = tuple(sid for sid in get_group(g) if sid in AOMC_IDS) or tuple(get_group(g))
    else:
        ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
    ck = _round_3min(datetime.now(timezone.utc))
    return ids, hours, ck


def _fmt_wl(wl, statuses):
    df = wl[wl["status"].isin(statuses)].copy()
    if df.empty:
        return df
    df["latest_time"] = df["latest_time"].apply(
        lambda t: t.strftime("%H:%MZ") if pd.notna(t) else "—")
    df["latest_flag_time"] = df["latest_flag_time"].apply(
        lambda t: t.strftime("%H:%MZ") if pd.notna(t) else "—")
    for c in ["minutes_since_last_flag", "minutes_since_last_report"]:
        df[c] = df[c].apply(lambda m: f"{m:.0f}" if m is not None and pd.notna(m) else "—")
    return df


# ===========================================================================
# SUMMARY
# ===========================================================================

with tab_summary:
    if not _HAVE_AOMC:
        st.error("AOMC catalog not loaded. Restart the app or contact your administrator.")
    else:
        all_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
        ck = _round_3min(datetime.now(timezone.utc))

        with st.spinner("Scanning network…"):
            wl = _scan(all_ids, float(_SCAN_HOURS), ck)

        if wl.empty:
            st.warning("No data returned. Check network connectivity.")
        else:
            cts = wl["status"].value_counts()
            nf = int(cts.get("FLAGGED", 0))
            ni = int(cts.get("INTERMITTENT", 0))
            nr = int(cts.get("RECOVERED", 0))
            nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
            nc = int(cts.get("CLEAN", 0))

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Clean", nc)
            c2.metric("Flagged ($)", nf)
            c3.metric("Missing", nm)
            c4.metric("Recovered", nr)
            c5.metric("Interm.", ni)
            st.caption(f"Last {_SCAN_HOURS}h · {len(wl)} stations · {datetime.now(timezone.utc):%H:%M:%S UTC}")

            st.divider()

            if nf == 0 and nm == 0:
                st.success(f"Network healthy. All {nc} stations reporting clean.")
            else:
                msg = []
                if nf: msg.append(f"**{nf}** flagged ($)")
                if ni: msg.append(f"**{ni}** intermittent")
                if nm: msg.append(f"**{nm}** missing")
                if nr: msg.append(f"**{nr}** recovered")
                st.warning(" · ".join(msg) + f" · **{nc}** clean (not listed)")

            # Flagged
            flagged = _fmt_wl(wl, ["FLAGGED"])
            if not flagged.empty:
                st.subheader(f"Flagged Stations ({len(flagged)})")
                st.dataframe(
                    flagged[["station", "name", "state", "probable_reason",
                             "flagged", "total", "flag_rate"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "probable_reason": "Reason",
                        "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                    }),
                    use_container_width=True, hide_index=True,
                    height=min(36 * len(flagged) + 38, 460),
                    column_config={
                        "Rate %": st.column_config.ProgressColumn(
                            min_value=0, max_value=100, format="%.0f%%"),
                    })

            # Intermittent
            inter = _fmt_wl(wl, ["INTERMITTENT"])
            if not inter.empty:
                st.subheader(f"Intermittent ({len(inter)})")
                st.dataframe(
                    inter[["station", "name", "state", "probable_reason"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "probable_reason": "Reason",
                    }),
                    use_container_width=True, hide_index=True,
                    height=min(36 * len(inter) + 38, 220))

            # Recovered
            recov = _fmt_wl(wl, ["RECOVERED"])
            if not recov.empty:
                st.subheader(f"Recovered ({len(recov)})")
                st.dataframe(
                    recov[["station", "name", "state", "minutes_since_last_flag"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "minutes_since_last_flag": "Min since $",
                    }),
                    use_container_width=True, hide_index=True,
                    height=min(36 * len(recov) + 38, 220))

            # Missing
            miss = _fmt_wl(wl, ["MISSING", "NO DATA"])
            if not miss.empty:
                st.subheader(f"Missing METARs ({len(miss)})")
                st.dataframe(
                    miss[["station", "name", "state", "missing",
                          "expected_hourly", "missing_hours_utc",
                          "minutes_since_last_report"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "missing": "Gaps", "expected_hourly": "Expected",
                        "missing_hours_utc": "Missing Hours",
                        "minutes_since_last_report": "Min since report",
                    }),
                    use_container_width=True, hide_index=True,
                    height=min(36 * len(miss) + 38, 460))

            st.divider()
            st.caption(
                f"**{nc} stations** reporting clean — not listed. "
                "Use the other tabs for detailed analysis."
            )


# ===========================================================================
# REPORTS
# ===========================================================================

with tab_reports:
    if not go:
        st.info("Configure report settings in the sidebar and click **Generate**.")
    else:
        try:
            if report_type == "1-min dashboard":
                stn = stations_list[0]
                with st.spinner(f"Fetching {stn}…"):
                    df = _fetch_1min(stn, start, end)
                if df.empty:
                    st.error("No data for this station/window.")
                else:
                    name = str(df["station_name"].iloc[0])
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Station", stn)
                    c2.metric("Name", _short_name(name))
                    c3.metric("Window", wlabel)
                    c4.metric("Observations", f"{len(df):,}")

                    with st.spinner("Rendering…"):
                        png = _render(build_report, df=df, window_label=wlabel,
                                      station_id=stn, station_name=name)
                    st.image(png, use_container_width=True)

                    c1, c2 = st.columns(2)
                    c1.download_button("Download PNG", png,
                                       f"{stn}_{wlabel.replace(' ', '')}.png",
                                       "image/png", use_container_width=True)
                    c2.download_button("Download CSV",
                                       df.to_csv(index=False).encode(),
                                       f"{stn}_{wlabel.replace(' ', '')}.csv",
                                       "text/csv", use_container_width=True)

                    with st.expander("Data preview (50 rows)"):
                        st.dataframe(df.head(50), use_container_width=True, height=300)
            else:
                with st.spinner("Fetching METARs…"):
                    metars = _fetch_metars(tuple(stations_list), start, end)
                if metars.empty:
                    st.error("No METARs for this selection/window.")
                else:
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Stations", metars["station"].nunique())
                    c2.metric("METARs", f"{len(metars):,}")
                    nflag = int(metars["has_maintenance"].sum())
                    c3.metric("Flagged", f"{nflag} ({nflag / len(metars) * 100:.0f}%)")

                    builders = {
                        "Maintenance ($)": (build_maintenance_report, "maintenance"),
                        "Flagged vs clean": (build_comparison_report, "comparison"),
                        "Missing METARs": (build_missing_report, "missing"),
                    }
                    builder, kind = builders[report_type]
                    with st.spinner("Rendering…"):
                        png = _render(builder, metars_df=metars,
                                      group_label=group_label, window_label=wlabel)
                    st.image(png, use_container_width=True)

                    slug = group_label.lower().replace(" ", "-").replace(",", "")
                    c1, c2 = st.columns(2)
                    c1.download_button("Download PNG", png,
                                       f"{slug}_{wlabel.replace(' ', '')}_{kind}.png",
                                       "image/png", use_container_width=True)
                    c2.download_button("Download CSV",
                                       metars.to_csv(index=False).encode(),
                                       f"{slug}_{wlabel.replace(' ', '')}_metars.csv",
                                       "text/csv", use_container_width=True)

                    with st.expander("METAR data preview (50 rows)"):
                        st.dataframe(metars.head(50), use_container_width=True, height=300)
        except Exception as e:
            logger.exception("Report generation failed")
            st.error(f"Could not generate report: {e}. Check station ID and time window.")


# ===========================================================================
# STATIONS
# ===========================================================================

with tab_stations:
    st.subheader("AOMC Federal Station Directory")
    st.caption(
        f"{len(AOMC_STATIONS)} stations from NCEI HOMR — "
        "NWS / FAA / DOD operated ASOS sites."
    )
    if _HAVE_AOMC:
        c1, c2 = st.columns([3, 1])
        with c1:
            q = st.text_input("Search", placeholder="ID, name, or county",
                              label_visibility="collapsed")
        with c2:
            states = sorted({s.get("state") for s in AOMC_STATIONS if s.get("state")})
            sf = st.selectbox("State", ["All"] + states, label_visibility="collapsed")

        rows = AOMC_STATIONS
        if sf != "All":
            rows = [s for s in rows if s.get("state") == sf]
        if q:
            qu = q.upper()
            rows = [s for s in rows
                    if qu in (s.get("id") or "").upper()
                    or qu in (s.get("name") or "").upper()
                    or qu in (s.get("county") or "").upper()]

        st.caption(f"{len(rows)} stations")
        df = pd.DataFrame([{
            "ICAO": s.get("id"), "Call": s.get("call"),
            "Name": s.get("name"), "State": s.get("state"),
            "County": s.get("county"), "Lat": s.get("lat"),
            "Lon": s.get("lon"), "Elev (ft)": s.get("elev_ft"),
            "WBAN": s.get("wban"), "Types": s.get("station_types"),
        } for s in rows])
        st.dataframe(df, use_container_width=True, height=580, hide_index=True)
        if rows:
            st.download_button("Download CSV", df.to_csv(index=False).encode(),
                               "aomc_stations.csv", "text/csv")
    else:
        st.error("Catalog not loaded.")


# ===========================================================================
# $ FLAGS
# ===========================================================================

with tab_flags:
    st.subheader("$ Maintenance Flag Watchlist")
    st.caption(
        "Stations with the $ maintenance-check indicator. "
        "The $ flag signals an out-of-tolerance condition — "
        "it does not mean the data is inaccurate."
    )
    if _HAVE_AOMC:
        ids, hours, ck = _scan_ui("fl")
        with st.spinner(f"Scanning {len(ids)} stations…"):
            wl_f = _scan(ids, float(hours), ck)
        if wl_f.empty:
            st.warning("No data returned. Verify station IDs are valid and the time window is within the last 30 days.")
        else:
            cts = wl_f["status"].value_counts()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Flagged", int(cts.get("FLAGGED", 0)))
            c2.metric("Intermittent", int(cts.get("INTERMITTENT", 0)))
            c3.metric("Recovered", int(cts.get("RECOVERED", 0)))
            c4.metric("Clean", int(cts.get("CLEAN", 0)))
            st.caption(f"Scanned {datetime.now(timezone.utc):%H:%M:%S UTC}")

            show_clean = st.checkbox("Include clean", key="fl_c")
            keep = ["FLAGGED", "INTERMITTENT", "RECOVERED"]
            if show_clean: keep.append("CLEAN")
            view = _fmt_wl(wl_f, keep)
            if not view.empty:
                st.dataframe(
                    view[["station", "name", "state",
                          "probable_reason", "flagged", "total", "flag_rate",
                          "latest_time", "latest_flag_time"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "probable_reason": "Reason",
                        "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                        "latest_time": "Latest", "latest_flag_time": "Last $",
                    }),
                    use_container_width=True, height=540, hide_index=True,
                    column_config={
                        "Rate %": st.column_config.ProgressColumn(
                            min_value=0, max_value=100, format="%.0f%%"),
                    })
                st.download_button("Download CSV", view.to_csv(index=False).encode(),
                                   f"flags_{hours}h.csv", "text/csv")
            with st.expander("What does $ mean?"):
                st.markdown("""
The `$` is the ASOS maintenance-check indicator. Sensor-specific codes
decoded from METAR remarks: RVRNO (RVR), PWINO (precip ID), PNO (precip
gauge), FZRANO (freezing rain), TSNO (lightning), VISNO (visibility),
CHINO (ceilometer). Most flags show "Internal check" — tolerance drift
with no specific sensor code in the remarks.
                """)


# ===========================================================================
# MISSING METARS
# ===========================================================================

with tab_missing:
    st.subheader("Missing METAR Monitor")
    st.caption(
        "Stations that missed scheduled hourly METARs. "
        "ASOS routine: one report per hour at ~HH:51Z. "
        "A silent station is more critical than a flagged one."
    )
    if _HAVE_AOMC:
        ids, hours, ck = _scan_ui("ms")
        with st.spinner(f"Scanning {len(ids)} stations…"):
            wl_m = _scan(ids, float(hours), ck)
        if wl_m.empty:
            st.warning("No data returned. Verify station IDs are valid and the time window is within the last 30 days.")
        else:
            cts = wl_m["status"].value_counts()
            nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
            c1, c2, c3 = st.columns(3)
            c1.metric("Missing", nm)
            c2.metric("Reporting", len(wl_m) - nm)
            c3.metric("Total", len(wl_m))
            st.caption(f"Scanned {datetime.now(timezone.utc):%H:%M:%S UTC}")

            show_ok = st.checkbox("Include reporting", key="ms_ok")
            view = _fmt_wl(wl_m, list(wl_m["status"].unique()) if show_ok else ["MISSING", "NO DATA"])
            if not view.empty:
                st.dataframe(
                    view[["station", "name", "state",
                          "missing", "expected_hourly", "missing_hours_utc",
                          "minutes_since_last_report"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "missing": "Gaps", "expected_hourly": "Expected",
                        "missing_hours_utc": "Missing Hours",
                        "minutes_since_last_report": "Min since report",
                    }),
                    use_container_width=True, height=540, hide_index=True)
                st.download_button("Download CSV", view.to_csv(index=False).encode(),
                                   f"missing_{hours}h.csv", "text/csv")
            with st.expander("Why do METARs go missing?"):
                st.markdown("""
Common causes: power outage, communication failure, station
decommissioned/seasonal, sensor cascade failure, or IEM ingestion lag.
                """)


# ===========================================================================
# FEDERAL FOOTER — citations, source chain, system metadata
# ===========================================================================

_now_footer = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
st.markdown(f"""
<div class="fed-footer">
  <div class="fed-footer-cite">
    <strong>Data Source Chain:</strong>
    NOAA / NCEI ASOS METAR archive →
    Iowa Environmental Mesonet (mesonet.agron.iastate.edu) →
    O.W.L. processing layer.
    <strong>Station Catalog:</strong> NCEI Historical Observing Metadata Repository (HOMR)
    asos-stations.txt — {len(AOMC_STATIONS):,} federally-operated AOMC stations
    (NWS / FAA / DOD).
  </div>
  <div class="fed-footer-meta">
    O.W.L. — OBSERVATION WATCH LOG · v1.0 ·
    SYSTEM TIME {_now_footer} ·
    <a href="https://github.com/consigcody94/asos-tools-py">SOURCE</a> ·
    <a href="https://www.ncei.noaa.gov">NCEI</a> ·
    <a href="https://www.weather.gov/asos/asostech">ASOS DOCS</a> ·
    <a href="https://mesonet.agron.iastate.edu">IEM</a>
  </div>
</div>
""", unsafe_allow_html=True)
