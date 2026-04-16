"""Streamlit dashboard for ASOS Tools.

Three header tabs:

  1. Reports       — generate per-station 1-min dashboards, maintenance
                     and flagged-vs-clean reports over a user-picked window.
  2. AOMC Browser  — searchable table of the ~920 federal ASOS stations.
  3. Live Watchlist — 4-hour operational scan showing which sites are
                     currently flagging "$" and which have recovered.
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from asos_tools import fetch_1min, fetch_metars
from asos_tools.report import (
    build_comparison_report,
    build_maintenance_report,
    build_report,
)
from asos_tools.stations import GROUPS, get_group, list_groups
from asos_tools.watchlist import build_watchlist, STATUS_ORDER

try:
    from asos_tools.stations import ALL_ASOS_STATIONS, search_stations
    _HAVE_CATALOG = bool(ALL_ASOS_STATIONS)
except ImportError:
    ALL_ASOS_STATIONS = []
    _HAVE_CATALOG = False

try:
    from asos_tools.stations import AOMC_STATIONS, AOMC_IDS, is_aomc
    _HAVE_AOMC = bool(AOMC_STATIONS)
except ImportError:
    AOMC_STATIONS = []
    AOMC_IDS = frozenset()
    _HAVE_AOMC = False


# ---------------------------------------------------------------------------
# Cached IEM fetches
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def _cached_fetch_1min(station: str, start: datetime, end: datetime):
    return fetch_1min(station, start, end)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_fetch_metars(stations_key: tuple, start: datetime, end: datetime):
    return fetch_metars(list(stations_key), start, end)


@st.cache_data(ttl=180, show_spinner=False)  # 3-minute TTL for operational freshness
def _cached_watchlist(station_ids: tuple, hours: float, end_iso: str):
    """Watchlist scan; ``end_iso`` keyed so cache invalidates per-minute."""
    end = datetime.fromisoformat(end_iso).replace(tzinfo=timezone.utc)
    # Recover the metadata from the catalog.
    meta_by_id = {s["id"]: s for s in AOMC_STATIONS}
    stations_with_meta = [meta_by_id.get(sid, {"id": sid}) for sid in station_ids]
    return build_watchlist(stations_with_meta, hours=hours, end=end)


# ---------------------------------------------------------------------------
# Page + theme toggle
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ASOS Tools · 1-minute surface obs",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Theme state — persists across reruns via session_state.
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

_DARK = st.session_state.theme == "dark"

# ---------------------------------------------------------------------------
# CSS — two complete palettes, injected based on the toggle.
# ---------------------------------------------------------------------------

# Shared layout rules (theme-independent).
_CSS_SHARED = """
section.main > div.block-container {
    padding-top: 2.5rem; padding-bottom: 3rem; max-width: 1400px;
}
[data-testid="stSidebar"] > div { padding-top: 1rem; }
[data-testid="stSidebar"] label {
    font-weight: 600 !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.5px !important;
    text-transform: uppercase !important;
}
h1 {
    font-weight: 800 !important;
    font-size: 2.2rem !important;
    letter-spacing: -0.02em;
    margin-bottom: 0.2rem !important;
}
h2, h3 { font-weight: 700 !important; margin-top: 1.5rem !important; }
p, .stMarkdown { font-size: 0.95rem; line-height: 1.55; }
.accent-bar {
    height: 4px; width: 60px;
    background: linear-gradient(90deg, #38bdf8, #a855f7);
    border-radius: 2px; margin-bottom: 0.6rem;
}
.eyebrow {
    font-size: 0.75rem; font-weight: 700;
    letter-spacing: 0.2em; text-transform: uppercase; margin-bottom: 0.3rem;
}
.lede { font-size: 1.05rem; line-height: 1.55; max-width: 900px; margin-bottom: 1.2rem; }
/* st.metric() handles KPIs natively — no custom chip CSS needed. */
[data-testid="stImage"] {
    min-height: 680px; border-radius: 8px; display: block; overflow: hidden;
}
[data-testid="stImage"] img { display: block; width: 100%; height: auto; }
[data-testid="stExpander"] details[open] > summary ~ div { animation: none; }
.stTabs [data-baseweb="tab-list"] { gap: 0.4rem; padding-bottom: 0.3rem; }
.stTabs [data-baseweb="tab"] {
    background: transparent; font-weight: 700 !important;
    font-size: 0.95rem !important; border-radius: 6px !important;
    padding: 0.6rem 1.2rem !important; letter-spacing: 0.02em;
}
.footer-note {
    font-size: 0.75rem; text-align: center;
    margin-top: 3rem; padding-top: 1rem;
}
.flag-strip-wrap { margin: 0.4rem 0 1.5rem 0; }
.flag-strip {
    display: flex; gap: 1px; height: 14px;
    border-radius: 3px; overflow: hidden;
}
.flag-strip-tick { flex: 1 1 auto; }
.flag-strip-tick.flagged { background: #f87171; }
.flag-strip-tick.clean { background: #34d399; }
.flag-strip-labels {
    display: flex; justify-content: space-between;
    font-size: 0.7rem; margin-top: 0.2rem; font-family: monospace;
}
"""

# Dark palette.
_CSS_DARK = """
[data-testid="stApp"],
[data-testid="stAppViewContainer"],
[data-testid="stMainBlockContainer"],
[data-testid="stVerticalBlockBorderWrapper"],
.main { background-color: #0b1220 !important; color: #e2e8f0 !important; }
[data-testid="stHeader"] { background-color: #0b1220 !important; }
[data-testid="stSidebar"] { background: #0f1729 !important; border-right: 1px solid #1e293b; }
[data-testid="stSidebar"] label { color: #e2e8f0 !important; }
h1 { color: #f8fafc !important; }
h2, h3 { color: #f1f5f9 !important; }
p, .stMarkdown { color: #cbd5e1 !important; }
.eyebrow { color: #38bdf8; }
.lede { color: #94a3b8; }
/* KPIs use st.metric() — auto-themed. */
.stButton > button {
    background: linear-gradient(180deg, #38bdf8, #0ea5e9);
    color: #001220 !important; font-weight: 700; border: none;
    box-shadow: 0 4px 14px rgba(14,165,233,0.3);
}
.stButton > button:hover {
    background: linear-gradient(180deg, #7dd3fc, #38bdf8);
}
.stDownloadButton > button {
    background: #1e293b; color: #f1f5f9 !important;
    border: 1px solid #334155;
}
.streamlit-expanderHeader {
    background: #111a2e !important; border: 1px solid #253356 !important;
    color: #e2e8f0 !important;
}
[data-testid="stDataFrame"] { border: 1px solid #253356; border-radius: 6px; }
[data-testid="stImage"] { background: #0f1729; border: 1px solid #1e293b; }
[data-testid="stAlert"] {
    background: #111a2e; border-left: 4px solid #38bdf8; color: #e2e8f0 !important;
}
.stTabs [data-baseweb="tab-list"] { border-bottom: 1px solid #1e293b; }
.stTabs [data-baseweb="tab"] { color: #94a3b8 !important; }
.stTabs [data-baseweb="tab"]:hover { background: #111a2e !important; color: #e2e8f0 !important; }
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: #1b2740 !important; color: #f8fafc !important;
    border-bottom: 2px solid #38bdf8 !important;
}
.footer-note { color: #64748b; border-top: 1px solid #1e293b; }
.flag-strip { border: 1px solid #253356; }
.flag-strip-labels { color: #94a3b8; }
"""

# Light palette.
_CSS_LIGHT = """
[data-testid="stApp"],
[data-testid="stAppViewContainer"],
[data-testid="stMainBlockContainer"],
[data-testid="stVerticalBlockBorderWrapper"],
.main { background-color: #ffffff !important; color: #1e293b !important; }
[data-testid="stHeader"] { background-color: #ffffff !important; }
[data-testid="stSidebar"] { background: #f8fafc !important; border-right: 1px solid #e2e8f0; }
[data-testid="stSidebar"] label { color: #1e293b !important; }
h1 { color: #0f172a !important; }
h2, h3 { color: #1e293b !important; }
p, .stMarkdown { color: #334155 !important; }
.eyebrow { color: #0284c7; }
.lede { color: #64748b; }
/* KPIs use st.metric() — auto-themed. */
.stButton > button {
    background: linear-gradient(180deg, #0ea5e9, #0284c7);
    color: #ffffff !important; font-weight: 700; border: none;
    box-shadow: 0 2px 8px rgba(14,165,233,0.25);
}
.stButton > button:hover {
    background: linear-gradient(180deg, #38bdf8, #0ea5e9);
}
.stDownloadButton > button {
    background: #ffffff; color: #1e293b !important;
    border: 1px solid #cbd5e1;
}
.streamlit-expanderHeader {
    background: #f1f5f9 !important; border: 1px solid #e2e8f0 !important;
    color: #1e293b !important;
}
[data-testid="stDataFrame"] { border: 1px solid #e2e8f0; border-radius: 6px; }
[data-testid="stImage"] { background: #f1f5f9; border: 1px solid #e2e8f0; }
[data-testid="stAlert"] {
    background: #f0f9ff; border-left: 4px solid #0ea5e9; color: #1e293b !important;
}
.stTabs [data-baseweb="tab-list"] { border-bottom: 1px solid #e2e8f0; }
.stTabs [data-baseweb="tab"] { color: #64748b !important; }
.stTabs [data-baseweb="tab"]:hover { background: #f1f5f9 !important; color: #1e293b !important; }
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: #e0f2fe !important; color: #0f172a !important;
    border-bottom: 2px solid #0284c7 !important;
}
.footer-note { color: #94a3b8; border-top: 1px solid #e2e8f0; }
.flag-strip { border: 1px solid #e2e8f0; }
.flag-strip-labels { color: #64748b; }

/* Force readable text on all Streamlit widgets in light mode. */
[data-testid="stApp"] * {
    color: inherit;
}
[data-baseweb="select"] span,
[data-baseweb="radio"] label,
[data-baseweb="checkbox"] label,
[data-testid="stWidgetLabel"] label,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stCaptionContainer"],
[data-testid="stText"] {
    color: #334155 !important;
}
[data-baseweb="input"] input,
[data-baseweb="textarea"] textarea {
    color: #1e293b !important;
    background: #ffffff !important;
    border-color: #cbd5e1 !important;
}
/* Sidebar widget text in light mode */
[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-baseweb="radio"] label,
[data-testid="stSidebar"] [data-baseweb="checkbox"] label,
[data-testid="stSidebar"] p {
    color: #334155 !important;
}
"""

st.markdown(
    f"<style>{_CSS_SHARED}\n{_CSS_DARK if _DARK else _CSS_LIGHT}</style>",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Header + theme toggle
# ---------------------------------------------------------------------------

def _toggle_theme():
    st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"

# Compact theme toggle in sidebar header — icon only, no full-width button.
col_brand, col_theme = st.sidebar.columns([4, 1])
with col_brand:
    st.sidebar.caption("ASOS Tools")
with col_theme:
    st.sidebar.button(
        "☀️" if _DARK else "🌙",
        on_click=_toggle_theme,
        key="theme_toggle",
        help="Switch to light mode" if _DARK else "Switch to dark mode",
    )
st.sidebar.markdown("---")

# Condensed header — one line title + one line subtitle (no accent bar/eyebrow).
st.markdown("# 🌤️ ASOS Tools")
st.caption(
    "Live ASOS station data, maintenance-flag monitoring, and missing-METAR "
    "tracking for 920 federal stations. Data: NOAA/NCEI via IEM."
)


def _kpis(items: list[tuple[str, str]]) -> None:
    """Render a row of KPI metrics using st.metric (auto-themes)."""
    cols = st.columns(len(items))
    for col, (label, value) in zip(cols, items):
        col.metric(label, value)


# Status emoji badges for tables.
_STATUS_BADGE = {
    "MISSING": "🔴 MISSING",
    "FLAGGED": "🟠 FLAGGED",
    "INTERMITTENT": "🟡 INTERMITTENT",
    "RECOVERED": "🟢 RECOVERED",
    "CLEAN": "✅ CLEAN",
    "NO DATA": "⚪ NO DATA",
}


def _badge_status(df: pd.DataFrame) -> pd.DataFrame:
    """Replace plain status text with emoji-prefixed badges."""
    if "status" in df.columns:
        df = df.copy()
        df["status"] = df["status"].map(lambda s: _STATUS_BADGE.get(s, s))
    if "Status" in df.columns:
        df = df.copy()
        df["Status"] = df["Status"].map(lambda s: _STATUS_BADGE.get(s, s))
    return df


def _render_to_bytes(builder_fn, **kwargs) -> bytes:
    tmp = Path(f".streamlit_tmp_{datetime.now().timestamp()}.png")
    try:
        builder_fn(out_path=tmp, **kwargs)
        return tmp.read_bytes()
    finally:
        if tmp.exists():
            tmp.unlink()


def _flag_strip_html(metars_df) -> str:
    if metars_df.empty:
        return ""
    metars_df = metars_df.sort_values("valid")
    ticks = "".join(
        f'<div class="flag-strip-tick {"flagged" if r else "clean"}"></div>'
        for r in metars_df["has_maintenance"]
    )
    first = metars_df["valid"].iloc[0].strftime("%Y-%m-%d %H:%MZ")
    last = metars_df["valid"].iloc[-1].strftime("%Y-%m-%d %H:%MZ")
    return (
        '<div class="flag-strip-wrap">'
        '<div class="flag-strip-labels" style="margin-bottom:0.3rem;">'
        '<span><span style="color:#34d399;">■</span> clean</span>'
        '<span style="color:#cbd5e1;">TIMELINE OF $ FLAGS</span>'
        '<span><span style="color:#f87171;">■</span> flagged $</span>'
        '</div>'
        f'<div class="flag-strip">{ticks}</div>'
        f'<div class="flag-strip-labels"><span>{first}</span><span>{last}</span></div>'
        '</div>'
    )


def _do_group_zip(stations_list, start, end, group_label, window_label) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        progress = st.progress(0.0, text="Generating per-station reports…")
        for i, stn in enumerate(stations_list):
            progress.progress((i + 0.5) / (len(stations_list) + 2),
                              text=f"Fetching {stn}…")
            try:
                df = _cached_fetch_1min(stn, start, end)
                if df.empty:
                    continue
                png = _render_to_bytes(build_report, df=df,
                                       window_label=window_label,
                                       station_id=stn,
                                       station_name=str(df["station_name"].iloc[0]))
                zf.writestr(f"{stn.lower()}_{window_label.replace(' ', '')}.png", png)
            except Exception as e:
                zf.writestr(f"{stn.lower()}_ERROR.txt", str(e))
        progress.progress((len(stations_list) + 0.5) / (len(stations_list) + 2),
                          text="Fetching METARs…")
        try:
            metars = _cached_fetch_metars(tuple(stations_list), start, end)
            if not metars.empty:
                for kind, builder in [("maintenance", build_maintenance_report),
                                      ("comparison", build_comparison_report)]:
                    png = _render_to_bytes(builder, metars_df=metars,
                                           group_label=group_label,
                                           window_label=window_label)
                    zf.writestr(f"{group_label.lower().replace(' ', '-')}_"
                                f"{window_label.replace(' ', '')}_{kind}.png", png)
                zf.writestr(
                    f"{group_label.lower().replace(' ', '-')}_metars.csv",
                    metars.to_csv(index=False).encode(),
                )
        except Exception as e:
            zf.writestr("GROUP_METAR_ERROR.txt", str(e))
        progress.progress(1.0, text="Done")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Header tabs
# ---------------------------------------------------------------------------

tab_reports, tab_browser, tab_watchlist, tab_missing = st.tabs([
    "📊 Reports",
    "🗂 Stations",
    "⚠️ $ Flags",
    "🚨 Missing",
])


# ===========================================================================
# TAB 1 — Reports (the original app)
# ===========================================================================

with tab_reports:
    st.sidebar.markdown("### Report type")
    report_type = st.sidebar.radio(
        "Report type",
        ["1-minute dashboard",
         "Maintenance flags ($)",
         "Flagged vs clean comparison"],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("### Station(s)")
    station_mode = st.sidebar.radio(
        "Station mode",
        ["Single station", "Preset group", "Custom list"],
        label_visibility="collapsed",
        horizontal=True,
    )

    stations_list: list[str] = []
    group_label: str = ""

    if station_mode == "Single station":
        aomc_only = False
        if _HAVE_AOMC:
            aomc_only = st.sidebar.toggle(
                f"AOMC federal ASOS only ({len(AOMC_STATIONS):,} sites)",
                value=True,
                help="Limit to the ~920 federally operated ASOS stations from NCEI HOMR.",
            )
        if _HAVE_CATALOG:
            pool = ([s for s in ALL_ASOS_STATIONS if s["id"] in AOMC_IDS]
                    if aomc_only and _HAVE_AOMC else ALL_ASOS_STATIONS)
            all_ids = [s["id"] for s in pool]
            default_idx = all_ids.index("KJFK") if "KJFK" in all_ids else 0
            sid = st.sidebar.selectbox(
                f"Pick a station ({len(all_ids):,} available)",
                all_ids,
                index=default_idx,
                format_func=lambda s: next(
                    (f"{s} — {r['name']} ({r.get('state','?')})"
                     for r in pool if r["id"] == s), s),
            )
        else:
            sid = st.sidebar.text_input("ICAO station ID", "KJFK").strip().upper()
        stations_list = [sid]
        group_label = sid

    elif station_mode == "Preset group":
        preset = st.sidebar.selectbox(
            "Group",
            list_groups(),
            format_func=lambda s: s.replace("_", " ").title(),
            index=list_groups().index("long_island") if "long_island" in list_groups() else 0,
        )
        stations_list = list(get_group(preset))
        group_label = preset.replace("_", " ").title()

    else:
        aomc_only_custom = False
        if _HAVE_AOMC:
            aomc_only_custom = st.sidebar.toggle(
                "AOMC federal ASOS only",
                value=True, key="aomc_custom",
            )
        if _HAVE_CATALOG:
            pool = ([s for s in ALL_ASOS_STATIONS if s["id"] in AOMC_IDS]
                    if aomc_only_custom and _HAVE_AOMC else ALL_ASOS_STATIONS)
            all_ids = [s["id"] for s in pool]
            picked = st.sidebar.multiselect(
                f"Pick any stations ({len(all_ids):,} available)",
                all_ids,
                default=["KJFK", "KLGA", "KEWR"],
                format_func=lambda s: next(
                    (f"{s} — {r['name']}" for r in pool if r["id"] == s), s),
            )
            stations_list = picked or ["KJFK"]
        else:
            raw = st.sidebar.text_area("Station IDs", "KJFK\nKLGA\nKEWR")
            stations_list = [s.strip().upper() for s in
                             raw.replace(",", "\n").splitlines() if s.strip()]
        if stations_list:
            first = stations_list[:3]
            group_label = " · ".join(first) + (" +…" if len(stations_list) > 3 else "")

    st.sidebar.caption(f"{len(stations_list)} station(s) selected")

    st.sidebar.markdown("### Time window")
    window_mode = st.sidebar.radio(
        "Window",
        ["Last 1 day", "Last 7 days", "Last 14 days", "Last 30 days", "Custom range"],
        index=1,
        label_visibility="collapsed",
    )
    if window_mode == "Custom range":
        today_utc = datetime.now(timezone.utc).date()
        col_a, col_b = st.sidebar.columns(2)
        with col_a:
            start_date = st.date_input("Start", value=today_utc - timedelta(days=7))
        with col_b:
            end_date = st.date_input("End", value=today_utc)
        if end_date <= start_date:
            st.sidebar.error("End must be after start.")
            st.stop()
        start = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
        end = datetime.combine(end_date, datetime.min.time(), tzinfo=timezone.utc)
        window_label = f"{(end - start).days} day"
    else:
        days_map = {"Last 1 day": 1, "Last 7 days": 7,
                    "Last 14 days": 14, "Last 30 days": 30}
        days = days_map[window_mode]
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        window_label = f"{days} day"

    st.sidebar.markdown("---")
    go = st.sidebar.button("Generate report", type="primary", use_container_width=True)
    zip_all = False
    if len(stations_list) > 1:
        zip_all = st.sidebar.button(
            "⬇ ZIP all (per-station + METAR)",
            use_container_width=True,
        )

    if not (go or zip_all):
        st.info(
            "👈 Pick a **report type**, **station(s)**, and **time window** "
            "in the sidebar, then click **Generate report**."
        )
        with st.expander("What each report shows", expanded=True):
            st.markdown("""
- **1-minute dashboard** — full-resolution temp, wind, gusts, pressure,
  visibility, precipitation for your window, with wind rose.
- **Maintenance flags ($)** — tracks which METARs end with the ASOS
  `$` *maintenance-check indicator*. Works for any station count.
- **Flagged vs clean comparison** — stacked breakdown per station and
  hour of day.
            """)
    else:
        try:
            if zip_all:
                with st.spinner("Building group ZIP…"):
                    zip_bytes = _do_group_zip(stations_list, start, end,
                                              group_label, window_label)
                st.success(f"Generated ZIP for {len(stations_list)} stations.")
                st.download_button(
                    "⬇ Download ZIP", data=zip_bytes,
                    file_name=f"{group_label.lower().replace(' ', '-')}_"
                              f"{window_label.replace(' ', '')}_reports.zip",
                    mime="application/zip", use_container_width=True,
                )
            elif report_type == "1-minute dashboard":
                if len(stations_list) > 1:
                    st.warning(
                        f"1-minute dashboard renders **one station per report**. "
                        f"Showing **{stations_list[0]}** — use *ZIP all* to get every station."
                    )
                station = stations_list[0]
                with st.spinner(f"Fetching 1-minute data for {station}…"):
                    df = _cached_fetch_1min(station, start, end)
                if df.empty:
                    st.error("No 1-minute data in this window.")
                else:
                    station_name = str(df["station_name"].iloc[0])
                    _kpis([
                        ("Station", f"{station}"),
                        ("Name", station_name),
                        ("Window", window_label),
                        ("Observations", f"{len(df):,}"),
                    ])

                    if _HAVE_AOMC and station in AOMC_IDS:
                        meta = next(s for s in AOMC_STATIONS if s["id"] == station)
                        with st.expander("Station metadata (NCEI HOMR)"):
                            c1, c2, c3 = st.columns(3)
                            with c1:
                                st.markdown(f"**Call sign:** `{meta.get('call') or '—'}`")
                                st.markdown(f"**WBAN:** `{meta.get('wban') or '—'}`")
                                st.markdown(f"**COOP ID:** `{meta.get('coop_id') or '—'}`")
                                st.markdown(f"**GHCN-D:** `{meta.get('ghcnd_id') or '—'}`")
                            with c2:
                                st.markdown(f"**State:** {meta.get('state') or '—'}")
                                st.markdown(f"**County:** {meta.get('county') or '—'}")
                                st.markdown(f"**Country:** {meta.get('country') or '—'}")
                                st.markdown(f"**UTC offset:** {meta.get('utc_offset_hr') or '—'}")
                            with c3:
                                lat, lon, elev = meta.get("lat"), meta.get("lon"), meta.get("elev_ft")
                                st.markdown(f"**Lat/Lon:** `{lat:.4f}, {lon:.4f}`"
                                            if lat and lon else "**Lat/Lon:** —")
                                st.markdown(f"**Elevation:** {elev} ft" if elev else "**Elevation:** —")
                                st.markdown(f"**Station types:** `{meta.get('station_types') or '—'}`")
                                st.markdown(f"**Begin date:** {meta.get('begin_date') or '—'}")

                    with st.spinner("Rendering report…"):
                        png = _render_to_bytes(build_report, df=df,
                                               window_label=window_label,
                                               station_id=station,
                                               station_name=station_name)
                    st.image(png, use_container_width=True, output_format="PNG")

                    c1, c2 = st.columns(2)
                    with c1:
                        st.download_button("⬇ Download PNG", data=png,
                                           file_name=f"{station.lower()}_{window_label.replace(' ', '')}.png",
                                           mime="image/png", use_container_width=True)
                    with c2:
                        st.download_button("⬇ Download CSV",
                                           data=df.to_csv(index=False).encode(),
                                           file_name=f"{station.lower()}_{window_label.replace(' ', '')}.csv",
                                           mime="text/csv", use_container_width=True)
                    with st.expander("Raw data (50 rows preview)"):
                        st.dataframe(df.head(50), use_container_width=True, height=300)
            else:
                with st.spinner("Fetching METARs…"):
                    metars = _cached_fetch_metars(tuple(stations_list), start, end)
                if metars.empty:
                    st.error("No METARs in this window.")
                else:
                    total = len(metars)
                    n_flag = int(metars["has_maintenance"].sum())
                    rate = n_flag / total * 100 if total else 0

                    _kpis([
                        ("Stations", str(metars["station"].nunique())),
                        ("METARs", f"{total:,}"),
                        ("Flagged $", f"{n_flag:,}"),
                        ("Flag Rate", f"{rate:.1f}%"),
                    ])

                    if len(stations_list) == 1 and total > 0:
                        st.markdown(_flag_strip_html(metars), unsafe_allow_html=True)

                    builder = (build_maintenance_report
                               if report_type.startswith("Maintenance")
                               else build_comparison_report)
                    with st.spinner("Rendering report…"):
                        png = _render_to_bytes(builder, metars_df=metars,
                                               group_label=group_label,
                                               window_label=window_label)
                    st.image(png, use_container_width=True, output_format="PNG")

                    kind = "maintenance" if report_type.startswith("Maintenance") else "comparison"
                    c1, c2 = st.columns(2)
                    with c1:
                        st.download_button("⬇ Download PNG", data=png,
                                           file_name=f"{group_label.lower().replace(' ', '-')}_"
                                                     f"{window_label.replace(' ', '')}_{kind}.png",
                                           mime="image/png", use_container_width=True)
                    with c2:
                        st.download_button("⬇ Download METAR CSV",
                                           data=metars.to_csv(index=False).encode(),
                                           file_name=f"{group_label.lower().replace(' ', '-')}_"
                                                     f"{window_label.replace(' ', '')}_metars.csv",
                                           mime="text/csv", use_container_width=True)
                    with st.expander(f"METARs in window (showing 50 of {len(metars):,})"):
                        st.dataframe(metars.head(50), use_container_width=True, height=300)

        except ValueError as e:
            st.error(f"Request failed: {e}")
        except Exception as e:
            st.error(f"Something went wrong: {e}")
            st.exception(e)


# ===========================================================================
# TAB 2 — AOMC Browser
# ===========================================================================

with tab_browser:
    st.markdown("### Federal ASOS Station Directory")
    st.markdown(
        "Browse the **920 AOMC-certified ASOS stations** operated by NWS, FAA, "
        "and DOD. This list comes from NCEI's Historical Observing Metadata "
        "Repository (HOMR) — the authoritative federal source. Use the search "
        "and state filter to find stations, then switch to the **Reports** tab "
        "to generate a dashboard for any of them."
    )
    if not _HAVE_AOMC:
        st.warning("AOMC catalog not bundled — run `deploy/build_aomc_catalog.py`.")
    else:
        col_q, col_st = st.columns([3, 1])
        with col_q:
            query = st.text_input("Search by ID, name, or county",
                                  "", placeholder="e.g. Kennedy, JFK, Chicago, NY",
                                  label_visibility="collapsed")
        with col_st:
            states = sorted({s.get("state") for s in AOMC_STATIONS if s.get("state")})
            state_filter = st.selectbox("Filter by state", ["All states"] + states,
                                        index=0, label_visibility="collapsed")

        rows = AOMC_STATIONS
        if state_filter != "All states":
            rows = [s for s in rows if s.get("state") == state_filter]
        if query:
            q = query.upper().strip()
            rows = [s for s in rows
                    if q in (s.get("id", "") or "").upper()
                    or q in (s.get("name", "") or "").upper()
                    or q in (s.get("county", "") or "").upper()]

        st.caption(f"Showing {len(rows):,} of {len(AOMC_STATIONS):,} stations.")
        browser_df = pd.DataFrame([{
            "ICAO": s.get("id"),
            "Call": s.get("call"),
            "Name": s.get("name"),
            "State": s.get("state"),
            "County": s.get("county"),
            "Lat": s.get("lat"),
            "Lon": s.get("lon"),
            "Elev (ft)": s.get("elev_ft"),
            "WBAN": s.get("wban"),
            "Types": s.get("station_types"),
            "Begin date": s.get("begin_date"),
        } for s in rows])
        st.dataframe(browser_df, use_container_width=True, height=600, hide_index=True)

        if len(rows) > 0:
            st.download_button("⬇ Download filtered station list",
                               data=browser_df.to_csv(index=False).encode(),
                               file_name="aomc_stations_filtered.csv",
                               mime="text/csv")

        with st.expander("What is AOMC?"):
            st.markdown("""
The **ASOS Operations and Monitoring Center** is the NWS entity responsible for
the national ASOS network. The ~920 stations in this directory are federally
commissioned, calibrated, and maintained by NWS / FAA / DOD. They are distinct
from the broader set of ~2,900+ automated weather stations (including
general-aviation AWOS sites) indexed by IEM.

**Source:** [NCEI Historical Observing Metadata Repository (HOMR)](https://www.ncei.noaa.gov/access/homr/)
— file `asos-stations.txt`, fetched and bundled at package-build time.
            """)


# ===========================================================================
# Shared scan logic for tabs 3 + 4
# ===========================================================================

def _scan_controls(key_prefix: str):
    """Render scan-window + scope controls. Returns (scan_ids, hours, now_key)."""
    col_h, col_scope, col_refresh = st.columns([2, 2, 1])
    with col_h:
        hours = st.selectbox(
            "Scan window",
            [1, 2, 4, 6, 12, 24],
            index=2,
            format_func=lambda h: f"Last {h} hour{'s' if h != 1 else ''}",
            key=f"{key_prefix}_hours",
        )
    with col_scope:
        scope = st.selectbox(
            "Scope",
            ["All AOMC stations (~920)", "Single state", "Preset group"],
            key=f"{key_prefix}_scope",
        )
    with col_refresh:
        now_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")
        if st.button("🔄 Refresh", use_container_width=True, key=f"{key_prefix}_refresh"):
            _cached_watchlist.clear()
            now_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    if scope == "Single state":
        states = sorted({s.get("state") for s in AOMC_STATIONS if s.get("state")})
        state_pick = st.selectbox("Pick a state", states,
                                  index=states.index("NY") if "NY" in states else 0,
                                  key=f"{key_prefix}_state")
        scan_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("state") == state_pick)
    elif scope == "Preset group":
        preset = st.selectbox("Pick a preset group", list_groups(),
                              format_func=lambda s: s.replace("_", " ").title(),
                              key=f"{key_prefix}_group")
        group = get_group(preset)
        scan_ids = tuple(sid for sid in group if sid in AOMC_IDS) or tuple(group)
    else:
        scan_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))

    return scan_ids, hours, now_key


def _run_scan(scan_ids, hours, now_key):
    """Execute the scan and return the watchlist DataFrame."""
    st.caption(f"Scanning {len(scan_ids):,} station(s), last {hours} hour(s). "
               f"Cache TTL 3 min.")
    with st.spinner(f"Scanning {len(scan_ids)} stations…"):
        return _cached_watchlist(scan_ids, float(hours), now_key)


def _format_display(wl):
    """Prepare a display-ready copy of the watchlist DataFrame."""
    display = wl.copy()
    display["latest_time"] = display["latest_time"].apply(
        lambda t: t.strftime("%Y-%m-%d %H:%MZ") if pd.notna(t) else "—")
    display["latest_flag_time"] = display["latest_flag_time"].apply(
        lambda t: t.strftime("%H:%MZ") if pd.notna(t) else "—")
    display["min_since_last_flag"] = display["minutes_since_last_flag"].apply(
        lambda m: f"{m:.0f}" if m is not None and pd.notna(m) else "—")
    display["min_since_last_report"] = display["minutes_since_last_report"].apply(
        lambda m: f"{m:.0f}" if m is not None and pd.notna(m) else "—")
    return display


# ===========================================================================
# TAB 3 — $ Maintenance Watchlist (flagged / intermittent / recovered)
# ===========================================================================

with tab_watchlist:
    st.markdown("### $ Maintenance Flag Watchlist")
    st.markdown(
        "Scans every AOMC station and shows which ones are currently "
        "reporting the **`$` maintenance-check indicator** at the end of "
        "their METARs. A trailing `$` means the ASOS station has self-detected "
        "a sensor problem — data from that site should be treated with caution. "
        "This tab filters to only the `$`-related statuses so you can focus on "
        "**who's degraded** and **who has come back clean**."
    )
    if not _HAVE_AOMC:
        st.warning("AOMC catalog not bundled.")
    else:
        scan_ids, hours, now_key = _scan_controls("wl")
        try:
            wl = _run_scan(scan_ids, hours, now_key)
        except Exception as e:
            st.error(f"Scan failed: {e}")
            st.stop()

        if wl.empty:
            st.warning("No METARs returned for this scope/window.")
        else:
            counts = wl["status"].value_counts()
            flagged = int(counts.get("FLAGGED", 0))
            intermittent = int(counts.get("INTERMITTENT", 0))
            recovered = int(counts.get("RECOVERED", 0))
            clean = int(counts.get("CLEAN", 0))

            _kpis([
                ("🟠 Flagged $", f"{flagged:,}"),
                ("🟡 Intermittent", f"{intermittent:,}"),
                ("🟢 Recovered", f"{recovered:,}"),
                ("✅ Clean", f"{clean:,}"),
                ("Total", f"{len(wl):,}"),
            ])

            # Default: show only $-related rows.
            show_clean = st.checkbox("Also show CLEAN stations",
                                     value=False, key="wl_show_clean")
            filtered = wl[wl["status"].isin(
                ["FLAGGED", "INTERMITTENT", "RECOVERED"]
                + (["CLEAN"] if show_clean else [])
            )]

            display = _format_display(filtered)
            display = display[[
                "station", "name", "state", "status",
                "probable_reason",
                "flagged", "total", "flag_rate",
                "latest_time", "latest_flag_time", "min_since_last_flag",
                "latest_metar",
            ]].rename(columns={
                "station": "Station", "name": "Name", "state": "State",
                "status": "Status", "probable_reason": "Probable Reason",
                "flagged": "$ Count", "total": "Total",
                "flag_rate": "Flag %", "latest_time": "Latest Report",
                "latest_flag_time": "Last $", "min_since_last_flag": "Min since $",
                "latest_metar": "Latest METAR",
            })

            display = _badge_status(display)
            st.caption(f"Scanned at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            st.dataframe(display, use_container_width=True, height=540,
                         hide_index=True,
                         column_config={
                             "Flag %": st.column_config.ProgressColumn(
                                 "Flag %", min_value=0, max_value=100, format="%.0f%%"),
                             "Latest METAR": st.column_config.TextColumn(
                                 "Latest METAR", width="large"),
                         })

            st.download_button(
                "⬇ Download $ watchlist CSV",
                data=filtered.to_csv(index=False).encode(),
                file_name=f"dollar_watchlist_last{hours}h_"
                          f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}.csv",
                mime="text/csv")

            with st.expander("What does $ mean? + How we decode it"):
                st.markdown("""
**The `$` maintenance-check indicator** is appended to the end of an ASOS
METAR when the station's internal self-test detects an out-of-tolerance
condition. It signals that maintenance is needed — it does **not**
necessarily mean the reported data is inaccurate.

**How we decode the probable reason:**

The METAR remarks section can contain specific sensor-down codes that
tell you exactly which subsystem triggered the flag:

| Code | Sensor | Meaning |
|---|---|---|
| `RVRNO` | RVR sensor | Runway Visual Range data not available |
| `PWINO` | Precip ID sensor | Present weather identification not available |
| `PNO` | Precip gauge | Precipitation amount not available |
| `FZRANO` | Freezing rain sensor | Freezing rain detection not available |
| `TSNO` | Lightning sensor | Thunderstorm / lightning detection not available |
| `VISNO [loc]` | Visibility sensor | Visibility at a secondary location not available |
| `CHINO [loc]` | Ceilometer | Cloud height indicator at a location not available |

**Most common case:** the `$` appears with NO specific sensor code. This
means the ASOS self-test found a component drifting out of tolerance
(calibration age, sensor wear, environmental contamination) but the
sensor is still reporting data. The **Probable Reason** column will
show "Internal check" for these.

**Status definitions:**

| Status | Meaning |
|---|---|
| **FLAGGED** | Latest METAR ends with `$` |
| **INTERMITTENT** | Mixed flags — latest clean but previous was flagged |
| **RECOVERED** | Was flagged earlier; **last 2 reports are clean** |
| **CLEAN** | Zero `$` flags in the scan window |
                """)


# ===========================================================================
# TAB 4 — Missing METARs (silent stations)
# ===========================================================================

with tab_missing:
    st.markdown("### Missing METAR Monitor")
    st.markdown(
        "Scans AOMC stations for **missing scheduled METARs** — hours where "
        "the station was expected to report but didn't. A silent station is "
        "operationally **more critical** than a `$`-flagged one: at least a "
        "flagged station is still reporting data. A missing station gives you "
        "nothing."
    )
    st.markdown(
        "> **ASOS routine schedule:** one METAR per hour at approximately "
        "**`HH:51Z`**. The scanner checks each fully-elapsed hour bucket in "
        "the window. If a bucket has zero METARs filed, it's counted as "
        "*missing*. The current in-progress hour is skipped (15-min grace) "
        "to avoid false alerts."
    )

    if not _HAVE_AOMC:
        st.warning("AOMC catalog not bundled.")
    else:
        scan_ids_m, hours_m, now_key_m = _scan_controls("miss")
        try:
            wl_m = _run_scan(scan_ids_m, hours_m, now_key_m)
        except Exception as e:
            st.error(f"Scan failed: {e}")
            st.stop()

        if wl_m.empty:
            st.warning("No data returned.")
        else:
            counts_m = wl_m["status"].value_counts()
            missing = int(counts_m.get("MISSING", 0))
            no_data = int(counts_m.get("NO DATA", 0))
            total_m = len(wl_m)
            reporting = total_m - missing - no_data

            _kpis([
                ("🔴 Missing", f"{missing:,}"),
                ("⚪ No Data", f"{no_data:,}"),
                ("✅ Reporting", f"{reporting:,}"),
                ("Total Scanned", f"{total_m:,}"),
            ])

            # Show only MISSING + NO DATA by default.
            show_reporting = st.checkbox(
                "Also show stations that ARE reporting",
                value=False, key="miss_show_reporting")
            if show_reporting:
                filtered_m = wl_m
            else:
                filtered_m = wl_m[wl_m["status"].isin(["MISSING", "NO DATA"])]

            display_m = _format_display(filtered_m)
            display_m = display_m[[
                "station", "name", "state", "status",
                "probable_reason",
                "missing", "expected_hourly", "missing_hours_utc",
                "min_since_last_report",
                "total",
                "latest_time",
                "latest_metar",
            ]].rename(columns={
                "station": "Station", "name": "Name", "state": "State",
                "status": "Status", "probable_reason": "Probable Reason",
                "missing": "Missing",
                "expected_hourly": "Expected", "missing_hours_utc": "Missing Hours (UTC)",
                "min_since_last_report": "Min since report",
                "total": "Reports received",
                "latest_time": "Last report time",
                "latest_metar": "Last METAR (if any)",
            })

            display_m = _badge_status(display_m)
            st.caption(f"Scanned at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            st.dataframe(display_m, use_container_width=True, height=540,
                         hide_index=True,
                         column_config={
                             "Missing": st.column_config.NumberColumn(
                                 "Missing",
                                 help="Hourly reports that did NOT arrive."),
                             "Expected": st.column_config.NumberColumn(
                                 "Expected",
                                 help="How many hourly METARs SHOULD have been filed in this window."),
                             "Missing Hours (UTC)": st.column_config.TextColumn(
                                 "Missing Hours (UTC)", width="medium",
                                 help="Specific UTC hour boundaries with no METAR."),
                             "Last METAR (if any)": st.column_config.TextColumn(
                                 "Last METAR (if any)", width="large"),
                         })

            st.download_button(
                "⬇ Download missing-METAR CSV",
                data=filtered_m.to_csv(index=False).encode(),
                file_name=f"missing_metars_last{hours_m}h_"
                          f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}.csv",
                mime="text/csv")

            with st.expander("Why do METARs go missing?"):
                st.markdown("""
**Common causes of missing METARs:**

| Cause | Notes |
|---|---|
| **Power outage** | Station lost electricity; no system to file reports |
| **Communication failure** | ASOS is running but the METAR can't reach the FAA/NWS network |
| **Station decommissioned** | Some remote / military sites report seasonally or have been taken offline |
| **IEM ingestion lag** | Rare — IEM may be behind on ingesting from NCEI |
| **Sensor cascade failure** | All sensors fail simultaneously; ASOS decides not to file |

**What controllers should check:**
1. Is the station on the NOTAM system? (outage formally announced)
2. Is the AWOS backup available for that airport?
3. Has the station historically had gaps? (check with a 24h or 7-day window)
4. If the station was missing but is now back, check the first post-gap METAR
   for accuracy — sensor drift can occur during extended outages.

**Columns explained:**
- **Missing** — count of hour buckets in the window with no METAR
- **Expected** — how many hourly METARs the station SHOULD have filed
- **Missing Hours (UTC)** — the specific hours that went unreported
- **Min since report** — minutes since ANY METAR was received (higher = more stale)
                """)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown(
    '<div class="footer-note">'
    'Source: <a href="https://github.com/consigcody94/asos-tools-py" '
    'style="color:#38bdf8;">github.com/consigcody94/asos-tools-py</a> · '
    'Data: NOAA/NCEI ASOS archive via '
    '<a href="https://mesonet.agron.iastate.edu/" style="color:#38bdf8;">IEM</a> · '
    'Catalog: <a href="https://www.ncei.noaa.gov/access/homr/" '
    'style="color:#38bdf8;">NCEI HOMR</a>'
    '</div>',
    unsafe_allow_html=True,
)
