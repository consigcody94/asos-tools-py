"""Streamlit dashboard for ASOS Tools.

Deployable on:
  * Hugging Face Spaces (streamlit SDK)
  * Streamlit Community Cloud
  * Railway / Fly.io (run with ``streamlit run app.py``)
"""

from __future__ import annotations

import io
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


# ---------------------------------------------------------------------------
# Page config and some CSS for a darker, more dashboard-y shell.
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ASOS Tools · 1-minute surface obs",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
section.main > div.block-container { padding-top: 1.5rem; }
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0b1220 0%, #111a2e 100%);
}
h1, h2, h3 { color: #f8fafc; }
.stMarkdown, .stCaption, label, p { color: #cbd5e1; }
.kpi-chip {
    background: #1b2740; border: 1px solid #2d3d5c;
    padding: 0.6rem 1rem; border-radius: 8px;
    display: inline-block; margin-right: 0.5rem;
}
.kpi-chip .label {
    font-size: 0.7rem; color: #94a3b8; letter-spacing: 1.5px;
}
.kpi-chip .value {
    font-size: 1.3rem; font-weight: 700; color: #f8fafc;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("ASOS Tools · 1-minute surface observations")
st.caption(
    "Fetch NCEI ASOS data for any date range and render a dashboard report. "
    "Data via NOAA/NCEI → Iowa Environmental Mesonet. "
    "[Source on GitHub](https://github.com/consigcody94/asos-tools-py)"
)


# ---------------------------------------------------------------------------
# Sidebar — input
# ---------------------------------------------------------------------------

mode = st.sidebar.radio(
    "Report type",
    [
        "Single station · 1-minute",
        "Station group · maintenance ($ flags)",
        "Station group · flagged vs clean",
    ],
)

st.sidebar.divider()

window_choices = {"1 day": 1, "7 day": 7, "14 day": 14, "30 day": 30}
window_label = st.sidebar.radio("Window", list(window_choices), index=1)
window_days = window_choices[window_label]

anchor_mode = st.sidebar.radio("Anchor", ["Now (UTC)", "Specific date"], index=0)
if anchor_mode == "Now (UTC)":
    anchor = datetime.now(timezone.utc)
else:
    anchor_date = st.sidebar.date_input(
        "Anchor date (UTC)",
        value=datetime.now(timezone.utc).date(),
    )
    anchor = datetime.combine(anchor_date, datetime.min.time(), tzinfo=timezone.utc)

start = anchor - timedelta(days=window_days)

st.sidebar.divider()

station: str | None = None
stations_list: list[str] | None = None
group_label: str | None = None

if mode == "Single station · 1-minute":
    station = st.sidebar.text_input("ICAO station ID", "KJFK").strip().upper()
    stations_list = [station]
else:
    source = st.sidebar.radio("Source", ["Preset group", "Custom list"])
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
        raw = st.sidebar.text_area(
            "Station IDs (one per line or comma-separated)",
            "KJFK\nKLGA\nKEWR",
        )
        stations_list = [
            s.strip().upper() for s in raw.replace(",", "\n").splitlines()
            if s.strip()
        ]
        group_label = " · ".join(stations_list) if stations_list else "Custom"

    with st.sidebar.expander("Stations in this group"):
        st.write(stations_list)

st.sidebar.divider()
go = st.sidebar.button("Generate report", type="primary", use_container_width=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if not go:
    st.info("Pick a report type and window in the sidebar, then click **Generate report**.")
    with st.expander("What this tool does", expanded=False):
        st.markdown(
            """
            - **Single station · 1-minute** — full-resolution temp, wind, pressure,
              visibility, and per-minute precipitation for the window you pick,
              rendered as a dark dashboard.
            - **Maintenance flags** — for a group of stations, tracks which
              reports end with the ASOS ``$`` maintenance-check indicator.
              Shows per-station flag rate, a station×time heatmap, an hourly
              flag-rate curve, and the latest flagged METARs.
            - **Flagged vs clean** — stacked comparison of how many METARs in
              the window carry the ``$`` flag versus how many are clean,
              broken down by station and by hour of day.
            """
        )
    st.stop()


def _chip(label: str, value: str) -> str:
    return (
        f'<span class="kpi-chip">'
        f'<div class="label">{label.upper()}</div>'
        f'<div class="value">{value}</div>'
        f'</span>'
    )


def _report_to_bytes(builder_fn, **kwargs) -> bytes:
    """Render a report to bytes (without touching disk)."""
    buf = io.BytesIO()
    tmp_path = Path(f".streamlit_tmp_{datetime.now().timestamp()}.png")
    try:
        builder_fn(out_path=tmp_path, **kwargs)
        buf.write(tmp_path.read_bytes())
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return buf.getvalue()


try:
    with st.spinner("Fetching data from IEM…"):
        if mode == "Single station · 1-minute":
            df = fetch_1min(station, start, anchor)
        else:
            metars = fetch_metars(stations_list, start, anchor)

    if mode == "Single station · 1-minute":
        if df.empty:
            st.error("No 1-minute data in this window. Try a different station or a wider window.")
            st.stop()

        station_name = str(df["station_name"].iloc[0])
        st.markdown(
            _chip("station", station) + _chip("window", window_label) +
            _chip("rows", f"{len(df):,}"),
            unsafe_allow_html=True,
        )

        with st.spinner("Rendering report…"):
            png = _report_to_bytes(
                build_report,
                df=df,
                window_label=window_label,
                station_id=station,
                station_name=station_name,
            )
        st.image(png, use_container_width=True)
        st.download_button(
            "⬇ Download PNG",
            data=png,
            file_name=f"{station.lower()}_{window_label.replace(' ', '')}.png",
            mime="image/png",
        )

        with st.expander("Show raw data (first 100 rows)"):
            st.dataframe(df.head(100), use_container_width=True)

    else:  # station-group METAR reports
        if metars.empty:
            st.error("No METARs in this window. Try a different group or window.")
            st.stop()

        total = len(metars)
        n_flag = int(metars["has_maintenance"].sum())
        rate = n_flag / total * 100

        st.markdown(
            _chip("stations", str(metars["station"].nunique())) +
            _chip("metars", f"{total:,}") +
            _chip("flagged $", f"{n_flag:,}") +
            _chip("flag rate", f"{rate:.1f}%"),
            unsafe_allow_html=True,
        )

        builder = (build_maintenance_report
                   if "maintenance" in mode else build_comparison_report)
        with st.spinner("Rendering report…"):
            png = _report_to_bytes(
                builder,
                metars_df=metars,
                group_label=group_label or "Group",
                window_label=window_label,
            )
        st.image(png, use_container_width=True)
        kind = "maintenance" if "maintenance" in mode else "comparison"
        st.download_button(
            "⬇ Download PNG",
            data=png,
            file_name=f"{(group_label or 'group').lower().replace(' ', '-')}_"
                      f"{window_label.replace(' ', '')}_{kind}.png",
            mime="image/png",
        )

        with st.expander("Show METAR rows (first 100)"):
            st.dataframe(metars.head(100), use_container_width=True)

except ValueError as e:
    st.error(f"Request failed: {e}")
except Exception as e:
    st.error(f"Something went wrong: {e}")
    st.exception(e)
