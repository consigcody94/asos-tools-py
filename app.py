"""Streamlit dashboard for ASOS Tools."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import streamlit as st

from asos_tools import fetch_1min, fetch_metars
from asos_tools.report import (
    build_comparison_report,
    build_maintenance_report,
    build_report,
)
from asos_tools.stations import GROUPS, get_group, list_groups

try:
    from asos_tools.stations import ALL_ASOS_STATIONS, search_stations
    _HAVE_CATALOG = bool(ALL_ASOS_STATIONS)
except ImportError:
    ALL_ASOS_STATIONS = []
    _HAVE_CATALOG = False


# =============================================================================
# Page + CSS
# =============================================================================

st.set_page_config(
    page_title="ASOS Tools · 1-minute surface obs",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
section.main > div.block-container {
    padding-top: 2.5rem; padding-bottom: 3rem; max-width: 1400px;
}
[data-testid="stSidebar"] {
    background: #0f1729; border-right: 1px solid #1e293b;
}
[data-testid="stSidebar"] > div { padding-top: 1rem; }
[data-testid="stSidebar"] label {
    color: #e2e8f0 !important;
    font-weight: 600 !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.5px !important;
    text-transform: uppercase !important;
}
h1 {
    color: #f8fafc !important; font-weight: 800 !important;
    font-size: 2.2rem !important; letter-spacing: -0.02em;
    margin-bottom: 0.2rem !important;
}
h2, h3 { color: #f1f5f9 !important; font-weight: 700 !important; margin-top: 1.5rem !important; }
p, .stMarkdown { color: #cbd5e1 !important; font-size: 0.95rem; line-height: 1.55; }
.accent-bar {
    height: 4px; width: 60px;
    background: linear-gradient(90deg, #38bdf8, #a855f7);
    border-radius: 2px; margin-bottom: 0.6rem;
}
.eyebrow {
    color: #38bdf8; font-size: 0.75rem; font-weight: 700;
    letter-spacing: 0.2em; text-transform: uppercase; margin-bottom: 0.3rem;
}
.lede {
    color: #94a3b8; font-size: 1.05rem; line-height: 1.55;
    max-width: 900px; margin-bottom: 1.8rem;
}
.chip-row { display: flex; gap: 0.55rem; flex-wrap: wrap; margin: 0.8rem 0 1.4rem 0; }
.kpi-chip {
    background: #111a2e; border: 1px solid #253356;
    border-left: 3px solid #38bdf8;
    padding: 0.55rem 1rem 0.55rem 0.85rem;
    border-radius: 8px; min-width: 150px;
}
.kpi-chip .label {
    font-size: 0.66rem; color: #94a3b8; letter-spacing: 0.15em;
    text-transform: uppercase; font-weight: 600; margin-bottom: 0.15rem;
}
.kpi-chip .value {
    font-size: 1.35rem; font-weight: 800; color: #f8fafc; line-height: 1.1;
}
.stButton > button {
    background: linear-gradient(180deg, #38bdf8, #0ea5e9);
    color: #001220 !important; font-weight: 700; border: none;
    padding: 0.65rem 1rem; border-radius: 8px;
    box-shadow: 0 4px 14px rgba(14,165,233,0.3);
}
.stButton > button:hover {
    background: linear-gradient(180deg, #7dd3fc, #38bdf8);
    box-shadow: 0 6px 20px rgba(14,165,233,0.5);
}
.stDownloadButton > button {
    background: #1e293b; color: #f1f5f9 !important;
    border: 1px solid #334155; font-weight: 600;
}
.stDownloadButton > button:hover { background: #334155; border-color: #64748b; }
.streamlit-expanderHeader {
    background: #111a2e !important; border: 1px solid #253356 !important;
    color: #e2e8f0 !important; font-weight: 600 !important;
}
[data-testid="stDataFrame"] { border: 1px solid #253356; border-radius: 6px; }
[data-testid="stAlert"] {
    background: #111a2e; border-left: 4px solid #38bdf8; color: #e2e8f0 !important;
}
.footer-note {
    color: #64748b; font-size: 0.75rem; text-align: center;
    margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #1e293b;
}
/* Flag-strip for single-station $ timeline */
.flag-strip-wrap { margin: 0.4rem 0 1.5rem 0; }
.flag-strip {
    display: flex; gap: 1px; height: 14px;
    border-radius: 3px; overflow: hidden;
    border: 1px solid #253356;
}
.flag-strip-tick { flex: 1 1 auto; }
.flag-strip-tick.flagged { background: #f87171; }
.flag-strip-tick.clean { background: #34d399; }
.flag-strip-labels {
    display: flex; justify-content: space-between;
    font-size: 0.7rem; color: #94a3b8; margin-top: 0.2rem;
    font-family: monospace;
}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Header
# =============================================================================

st.markdown('<div class="accent-bar"></div>', unsafe_allow_html=True)
st.markdown('<div class="eyebrow">NCEI · ASOS · 1-minute surface observations</div>',
            unsafe_allow_html=True)
st.markdown("# ASOS Tools")
st.markdown(
    '<div class="lede">'
    'Pull 1-minute weather station data for any date range and render '
    'publication-quality dashboard reports. Works on any single station or '
    'a group; the same maintenance-flag (<code>$</code>) analytics apply '
    'either way. Live data from NOAA/NCEI via the Iowa Environmental Mesonet.'
    '</div>',
    unsafe_allow_html=True,
)


# =============================================================================
# Sidebar
# =============================================================================

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
    if _HAVE_CATALOG:
        all_ids = [s["id"] for s in ALL_ASOS_STATIONS]
        default_idx = all_ids.index("KJFK") if "KJFK" in all_ids else 0
        sid = st.sidebar.selectbox(
            "Pick any of 2,929 ASOS sites",
            all_ids,
            index=default_idx,
            format_func=lambda s: next(
                (f"{s} — {r['name']} ({r.get('state','?')})"
                 for r in ALL_ASOS_STATIONS if r["id"] == s), s),
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
else:  # custom list
    if _HAVE_CATALOG:
        all_ids = [s["id"] for s in ALL_ASOS_STATIONS]
        picked = st.sidebar.multiselect(
            "Pick any stations",
            all_ids,
            default=["KJFK", "KLGA", "KEWR"],
            format_func=lambda s: next(
                (f"{s} — {r['name']}" for r in ALL_ASOS_STATIONS if r["id"] == s), s),
        )
        stations_list = picked or ["KJFK"]
    else:
        raw = st.sidebar.text_area("Station IDs", "KJFK\nKLGA\nKEWR")
        stations_list = [s.strip().upper() for s in raw.replace(",", "\n").splitlines() if s.strip()]
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


# =============================================================================
# Helpers
# =============================================================================

def _chip(label: str, value: str) -> str:
    return (f'<div class="kpi-chip"><div class="label">{label}</div>'
            f'<div class="value">{value}</div></div>')


def _render_to_bytes(builder_fn, **kwargs) -> bytes:
    tmp = Path(f".streamlit_tmp_{datetime.now().timestamp()}.png")
    try:
        builder_fn(out_path=tmp, **kwargs)
        return tmp.read_bytes()
    finally:
        if tmp.exists():
            tmp.unlink()


def _flag_strip_html(metars_df) -> str:
    """Render a thin horizontal strip coloring each METAR red (flagged) or green (clean)."""
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
                df = fetch_1min(stn, start, end)
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
            metars = fetch_metars(stations_list, start, end)
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


# =============================================================================
# Landing state
# =============================================================================

if not (go or zip_all):
    st.info(
        "👈 Pick a **report type**, **station(s)**, and **time window** "
        "in the sidebar, then click **Generate report**."
    )
    with st.expander("What each report shows", expanded=True):
        st.markdown("""
- **1-minute dashboard** — full-resolution temp, wind, gusts, pressure,
  visibility, precipitation for your window, with wind rose.
- **Maintenance flags ($)** — tracks which METAR reports end with the ASOS
  `$` *maintenance-check indicator*. Works for a single station (shows a
  timeline of its own flagged vs clean reports) or a group (per-station
  flag rate, station × time heatmap).
- **Flagged vs clean comparison** — stacked breakdown of flagged vs clean
  METARs over time, per-station, and by hour of day.
        """)
    st.markdown(
        '<div class="footer-note">'
        'Source: <a href="https://github.com/consigcody94/asos-tools-py" '
        'style="color:#38bdf8;">github.com/consigcody94/asos-tools-py</a> · '
        'Data: NOAA/NCEI ASOS archive via '
        '<a href="https://mesonet.agron.iastate.edu/" style="color:#38bdf8;">IEM</a>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.stop()


# =============================================================================
# Actions
# =============================================================================

try:
    if zip_all:
        with st.spinner("Building group ZIP…"):
            zip_bytes = _do_group_zip(stations_list, start, end,
                                      group_label, window_label)
        st.success(f"Generated ZIP for {len(stations_list)} stations over {window_label}.")
        st.download_button(
            "⬇ Download ZIP",
            data=zip_bytes,
            file_name=f"{group_label.lower().replace(' ', '-')}_"
                      f"{window_label.replace(' ', '')}_reports.zip",
            mime="application/zip",
            use_container_width=True,
        )
        st.stop()

    # 1-minute dashboard.
    if report_type == "1-minute dashboard":
        if len(stations_list) > 1:
            st.warning(
                "1-minute dashboard renders **one station per report**. "
                f"Showing **{stations_list[0]}** — use *ZIP all* in the sidebar "
                "to get every station in your selection."
            )
        station = stations_list[0]

        with st.spinner(f"Fetching 1-minute data for {station}…"):
            df = fetch_1min(station, start, end)
        if df.empty:
            st.error("No 1-minute data in this window. Try a different station or wider window.")
            st.stop()

        station_name = str(df["station_name"].iloc[0])
        st.markdown(
            f'<div class="chip-row">'
            f'{_chip("station", station)}'
            f'{_chip("name", station_name)}'
            f'{_chip("window", window_label)}'
            f'{_chip("rows", f"{len(df):,}")}'
            f'</div>',
            unsafe_allow_html=True,
        )
        with st.spinner("Rendering report…"):
            png = _render_to_bytes(build_report, df=df,
                                   window_label=window_label,
                                   station_id=station, station_name=station_name)
        st.image(png, use_container_width=True)

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

    # Maintenance flags OR comparison — both use fetch_metars.
    else:
        with st.spinner("Fetching METARs…"):
            metars = fetch_metars(stations_list, start, end)
        if metars.empty:
            st.error("No METARs in this window. Try a different station/window.")
            st.stop()

        total = len(metars)
        n_flag = int(metars["has_maintenance"].sum())
        rate = n_flag / total * 100 if total else 0

        st.markdown(
            f'<div class="chip-row">'
            f'{_chip("stations", str(metars["station"].nunique()))}'
            f'{_chip("metars", f"{total:,}")}'
            f'{_chip("flagged $", f"{n_flag:,}")}'
            f'{_chip("flag rate", f"{rate:.1f}%")}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Single-station flag timeline strip — added for single-station mode.
        if len(stations_list) == 1 and total > 0:
            st.markdown(_flag_strip_html(metars), unsafe_allow_html=True)

        # Main report (works for 1 or N stations).
        builder = (build_maintenance_report
                   if report_type.startswith("Maintenance")
                   else build_comparison_report)
        with st.spinner("Rendering report…"):
            png = _render_to_bytes(builder, metars_df=metars,
                                   group_label=group_label,
                                   window_label=window_label)
        st.image(png, use_container_width=True)

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

st.markdown(
    '<div class="footer-note">'
    'Live app source: <a href="https://github.com/consigcody94/asos-tools-py" '
    'style="color:#38bdf8;">github.com/consigcody94/asos-tools-py</a>'
    '</div>',
    unsafe_allow_html=True,
)
