"""Streamlit dashboard for ASOS Tools.

Deployable on Hugging Face Spaces (docker SDK, port 7860),
Streamlit Community Cloud, Railway, Fly.io, etc.
"""

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
    _HAVE_CATALOG = True
except ImportError:
    ALL_ASOS_STATIONS = []
    _HAVE_CATALOG = False


# =============================================================================
# Page + global CSS
# =============================================================================

st.set_page_config(
    page_title="ASOS Tools · 1-minute surface obs",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS layered on top of Streamlit's dark theme (.streamlit/config.toml).
st.markdown("""
<style>
/* Wider content column, more breathing room. */
section.main > div.block-container {
    padding-top: 2.5rem;
    padding-bottom: 3rem;
    max-width: 1400px;
}

/* Sidebar — deeper slate with subtle divider. */
[data-testid="stSidebar"] {
    background: #0f1729;
    border-right: 1px solid #1e293b;
}
[data-testid="stSidebar"] > div { padding-top: 1rem; }
[data-testid="stSidebar"] .stRadio > label,
[data-testid="stSidebar"] .stSelectbox > label,
[data-testid="stSidebar"] .stTextInput > label,
[data-testid="stSidebar"] .stTextArea > label,
[data-testid="stSidebar"] .stDateInput > label {
    color: #e2e8f0 !important;
    font-weight: 600;
    font-size: 0.85rem;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}

/* Headings. */
h1 {
    color: #f8fafc !important;
    font-weight: 800 !important;
    font-size: 2.2rem !important;
    letter-spacing: -0.02em;
    margin-bottom: 0.2rem !important;
}
h2, h3 {
    color: #f1f5f9 !important;
    font-weight: 700 !important;
    margin-top: 1.5rem !important;
}

/* Body text. */
p, .stMarkdown {
    color: #cbd5e1 !important;
    font-size: 0.95rem;
    line-height: 1.55;
}

/* Accent bar above the page title. */
.accent-bar {
    height: 4px;
    width: 60px;
    background: linear-gradient(90deg, #38bdf8, #a855f7);
    border-radius: 2px;
    margin-bottom: 0.6rem;
}

/* Eyebrow tag above title. */
.eyebrow {
    color: #38bdf8;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    margin-bottom: 0.3rem;
}

/* Caption directly under title. */
.lede {
    color: #94a3b8;
    font-size: 1.05rem;
    line-height: 1.55;
    max-width: 900px;
    margin-bottom: 1.8rem;
}

/* KPI chip row above reports. */
.chip-row { display: flex; gap: 0.55rem; flex-wrap: wrap; margin: 0.8rem 0 1.4rem 0; }
.kpi-chip {
    background: #111a2e;
    border: 1px solid #253356;
    border-left: 3px solid #38bdf8;
    padding: 0.55rem 1rem 0.55rem 0.85rem;
    border-radius: 8px;
    min-width: 150px;
}
.kpi-chip .label {
    font-size: 0.66rem;
    color: #94a3b8;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    font-weight: 600;
    margin-bottom: 0.15rem;
}
.kpi-chip .value {
    font-size: 1.35rem;
    font-weight: 800;
    color: #f8fafc;
    line-height: 1.1;
}

/* Buttons — prominent primary. */
.stButton > button {
    background: linear-gradient(180deg, #38bdf8, #0ea5e9);
    color: #001220 !important;
    font-weight: 700;
    border: none;
    padding: 0.65rem 1rem;
    border-radius: 8px;
    box-shadow: 0 4px 14px rgba(14,165,233,0.3);
}
.stButton > button:hover {
    background: linear-gradient(180deg, #7dd3fc, #38bdf8);
    box-shadow: 0 6px 20px rgba(14,165,233,0.5);
}
.stDownloadButton > button {
    background: #1e293b;
    color: #f1f5f9 !important;
    border: 1px solid #334155;
    font-weight: 600;
}
.stDownloadButton > button:hover {
    background: #334155;
    border-color: #64748b;
}

/* Expanders. */
.streamlit-expanderHeader {
    background: #111a2e !important;
    border: 1px solid #253356 !important;
    color: #e2e8f0 !important;
    font-weight: 600 !important;
}

/* Dataframes — make sure rows are readable. */
[data-testid="stDataFrame"] { border: 1px solid #253356; border-radius: 6px; }

/* Info / warning / error boxes — bolder text, dark backgrounds. */
[data-testid="stAlert"] {
    background: #111a2e;
    border-left: 4px solid #38bdf8;
    color: #e2e8f0 !important;
}

/* Footer note. */
.footer-note {
    color: #64748b;
    font-size: 0.75rem;
    text-align: center;
    margin-top: 3rem;
    padding-top: 1rem;
    border-top: 1px solid #1e293b;
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
    'publication-quality dashboard reports. Live data from NOAA/NCEI via the '
    'Iowa Environmental Mesonet. Choose a single station, a preset region, '
    'or build a custom comparison across any ASOS sites.'
    '</div>',
    unsafe_allow_html=True,
)


# =============================================================================
# Sidebar — input
# =============================================================================

st.sidebar.markdown("### Report")

mode = st.sidebar.radio(
    "Type",
    [
        "Single station · 1-minute",
        "Station group · maintenance ($ flags)",
        "Station group · flagged vs clean",
    ],
    label_visibility="collapsed",
)

st.sidebar.markdown("### Time window")
window_mode = st.sidebar.radio(
    "Range",
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
    anchor_mode = st.sidebar.radio("Ending", ["Now (UTC)", "Specific date"], index=0,
                                   horizontal=True)
    if anchor_mode == "Now (UTC)":
        end = datetime.now(timezone.utc)
    else:
        anchor_date = st.sidebar.date_input(
            "Anchor date (UTC)",
            value=datetime.now(timezone.utc).date(),
        )
        end = datetime.combine(anchor_date, datetime.min.time(), tzinfo=timezone.utc)
    days_map = {"Last 1 day": 1, "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30}
    days = days_map[window_mode]
    start = end - timedelta(days=days)
    window_label = f"{days} day"


# --- Station / group selector ------------------------------------------------

st.sidebar.markdown("### Station(s)")
station: str | None = None
stations_list: list[str] | None = None
group_label: str | None = None

if mode == "Single station · 1-minute":
    if _HAVE_CATALOG and ALL_ASOS_STATIONS:
        all_ids = [s["id"] for s in ALL_ASOS_STATIONS]
        default_idx = all_ids.index("KJFK") if "KJFK" in all_ids else 0
        station = st.sidebar.selectbox(
            "Pick a station (typeable, ~900 sites)",
            all_ids,
            index=default_idx,
            format_func=lambda sid: next(
                (f"{sid} — {s['name']} ({s.get('state','?')})"
                 for s in ALL_ASOS_STATIONS if s["id"] == sid),
                sid,
            ),
        )
    else:
        station = st.sidebar.text_input("ICAO station ID", "KJFK").strip().upper()
    stations_list = [station]
else:
    source = st.sidebar.radio("Source", ["Preset group", "Custom list"],
                              horizontal=True)
    if source == "Preset group":
        preset = st.sidebar.selectbox(
            "Group",
            list_groups(),
            format_func=lambda s: s.replace("_", " ").title(),
            index=list_groups().index("long_island")
            if "long_island" in list_groups() else 0,
        )
        stations_list = list(get_group(preset))
        group_label = preset.replace("_", " ").title()
    else:
        if _HAVE_CATALOG and ALL_ASOS_STATIONS:
            all_ids = [s["id"] for s in ALL_ASOS_STATIONS]
            picked = st.sidebar.multiselect(
                "Pick any stations",
                all_ids,
                default=["KJFK", "KLGA", "KEWR"],
                format_func=lambda sid: next(
                    (f"{sid} — {s['name']}"
                     for s in ALL_ASOS_STATIONS if s["id"] == sid),
                    sid,
                ),
            )
            stations_list = picked or ["KJFK", "KLGA", "KEWR"]
        else:
            raw = st.sidebar.text_area(
                "Station IDs (one per line or comma-separated)",
                "KJFK\nKLGA\nKEWR",
            )
            stations_list = [
                s.strip().upper() for s in raw.replace(",", "\n").splitlines()
                if s.strip()
            ]
        group_label = " · ".join(stations_list[:4]) + (
            " +…" if len(stations_list) > 4 else ""
        ) if stations_list else "Custom"

    if stations_list:
        with st.sidebar.expander(f"Stations in this group ({len(stations_list)})"):
            st.write(stations_list)

st.sidebar.markdown("---")
go = st.sidebar.button("Generate report", type="primary", use_container_width=True)
if mode == "Single station · 1-minute":
    zip_all = False
else:
    zip_all = st.sidebar.button(
        "⬇ Download group ZIP (per-station + metar)",
        use_container_width=True,
    )


# =============================================================================
# Main
# =============================================================================

def _chip(label: str, value: str) -> str:
    return (
        f'<div class="kpi-chip">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        f'</div>'
    )


def _render_report_to_bytes(builder_fn, **kwargs) -> bytes:
    tmp_path = Path(f".streamlit_tmp_{datetime.now().timestamp()}.png")
    try:
        builder_fn(out_path=tmp_path, **kwargs)
        return tmp_path.read_bytes()
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _do_group_zip(stations_list: list[str], start: datetime, end: datetime,
                  group_label: str, window_label: str) -> bytes:
    """Bundle per-station 1-min reports + group maintenance + comparison into a ZIP."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # 1-minute per-station reports
        progress = st.progress(0.0, text="Generating per-station reports…")
        for i, stn in enumerate(stations_list):
            progress.progress(
                (i + 0.5) / (len(stations_list) + 2),
                text=f"Fetching {stn}…",
            )
            try:
                df = fetch_1min(stn, start, end)
                if df.empty:
                    continue
                png = _render_report_to_bytes(
                    build_report,
                    df=df,
                    window_label=window_label,
                    station_id=stn,
                    station_name=str(df["station_name"].iloc[0]),
                )
                zf.writestr(f"{stn.lower()}_{window_label.replace(' ', '')}.png", png)
            except Exception as e:
                zf.writestr(f"{stn.lower()}_ERROR.txt", str(e))
        # Group METAR reports.
        progress.progress(
            (len(stations_list) + 0.5) / (len(stations_list) + 2),
            text="Fetching group METARs…",
        )
        try:
            metars = fetch_metars(stations_list, start, end)
            if not metars.empty:
                for kind, builder in [
                    ("maintenance", build_maintenance_report),
                    ("comparison", build_comparison_report),
                ]:
                    png = _render_report_to_bytes(
                        builder,
                        metars_df=metars,
                        group_label=group_label,
                        window_label=window_label,
                    )
                    zf.writestr(
                        f"{group_label.lower().replace(' ', '-')}_"
                        f"{window_label.replace(' ', '')}_{kind}.png",
                        png,
                    )
                zf.writestr(
                    f"{group_label.lower().replace(' ', '-')}_metars.csv",
                    metars.to_csv(index=False).encode(),
                )
        except Exception as e:
            zf.writestr("GROUP_METAR_ERROR.txt", str(e))
        progress.progress(1.0, text="Done")
    return buf.getvalue()


# --- Landing state ----------------------------------------------------------

if not (go or zip_all):
    st.info(
        "← Pick a report type, a time window, and a station (or group) "
        "in the sidebar, then click **Generate report**."
    )
    with st.expander("What each report shows", expanded=True):
        st.markdown(
            """
- **Single station · 1-minute** — full-resolution temperature, wind speed +
  gusts, station pressure, visibility, and per-minute precipitation for the
  window you pick, rendered as a dark dashboard with a wind rose.
- **Maintenance flags** — for a group of stations, tracks which METAR reports
  end with the ASOS `$` *maintenance-check indicator*. Shows per-station flag
  rate, a station × time heatmap, an hourly flag-rate timeline, and the most
  recent flagged METARs.
- **Flagged vs clean** — stacked breakdown of how many METARs in the window
  carry the `$` flag versus how many are clean, per-station and by hour of day.
            """
        )
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


# --- Action ----------------------------------------------------------------

try:
    if zip_all:
        with st.spinner("Building group ZIP…"):
            zip_bytes = _do_group_zip(stations_list, start, end,
                                      group_label or "group", window_label)
        st.success(f"Generated ZIP for {len(stations_list)} stations "
                   f"across {window_label}.")
        st.download_button(
            "⬇ Download ZIP",
            data=zip_bytes,
            file_name=f"{(group_label or 'group').lower().replace(' ', '-')}_"
                      f"{window_label.replace(' ', '')}_reports.zip",
            mime="application/zip",
            use_container_width=True,
        )
        st.stop()

    with st.spinner("Fetching data from IEM…"):
        if mode == "Single station · 1-minute":
            df = fetch_1min(station, start, end)
        else:
            metars = fetch_metars(stations_list, start, end)

    if mode == "Single station · 1-minute":
        if df.empty:
            st.error("No 1-minute data in this window. Try a different station or wider window.")
            st.stop()

        station_name = str(df["station_name"].iloc[0])
        st.markdown(
            f'<div class="chip-row">'
            + _chip("station", station)
            + _chip("name", station_name)
            + _chip("window", window_label)
            + _chip("rows", f"{len(df):,}")
            + '</div>',
            unsafe_allow_html=True,
        )

        with st.spinner("Rendering report…"):
            png = _render_report_to_bytes(
                build_report,
                df=df,
                window_label=window_label,
                station_id=station,
                station_name=station_name,
            )
        st.image(png, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "⬇ Download PNG",
                data=png,
                file_name=f"{station.lower()}_{window_label.replace(' ', '')}.png",
                mime="image/png",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "⬇ Download CSV",
                data=df.to_csv(index=False).encode(),
                file_name=f"{station.lower()}_{window_label.replace(' ', '')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with st.expander("Raw data (first 200 rows)"):
            st.dataframe(df.head(200), use_container_width=True)

    else:  # station-group METAR reports
        if metars.empty:
            st.error("No METARs in this window. Try a different group or window.")
            st.stop()

        total = len(metars)
        n_flag = int(metars["has_maintenance"].sum())
        rate = n_flag / total * 100

        st.markdown(
            f'<div class="chip-row">'
            + _chip("stations", str(metars["station"].nunique()))
            + _chip("metars", f"{total:,}")
            + _chip("flagged $", f"{n_flag:,}")
            + _chip("flag rate", f"{rate:.1f}%")
            + '</div>',
            unsafe_allow_html=True,
        )

        builder = (build_maintenance_report
                   if "maintenance" in mode else build_comparison_report)
        with st.spinner("Rendering report…"):
            png = _render_report_to_bytes(
                builder,
                metars_df=metars,
                group_label=group_label or "Group",
                window_label=window_label,
            )
        st.image(png, use_container_width=True)

        kind = "maintenance" if "maintenance" in mode else "comparison"
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "⬇ Download PNG",
                data=png,
                file_name=f"{(group_label or 'group').lower().replace(' ', '-')}_"
                          f"{window_label.replace(' ', '')}_{kind}.png",
                mime="image/png",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "⬇ Download METAR CSV",
                data=metars.to_csv(index=False).encode(),
                file_name=f"{(group_label or 'group').lower().replace(' ', '-')}_"
                          f"{window_label.replace(' ', '')}_metars.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with st.expander(f"METARs in window ({len(metars):,} rows, first 200)"):
            st.dataframe(metars.head(200), use_container_width=True)

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
