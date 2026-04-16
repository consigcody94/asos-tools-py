"""ASOS Tools — operational dashboard for the federal ASOS network."""

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
from asos_tools._missing_report import build_missing_report
from asos_tools.nws import get_current_conditions
from asos_tools.stations import GROUPS, get_group, list_groups
from asos_tools.watchlist import build_watchlist

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


# ---------------------------------------------------------------------------
# Cached fetches
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def _fetch_1min(station, start, end):
    return fetch_1min(station, start, end)

@st.cache_data(ttl=600, show_spinner=False)
def _fetch_metars(stations_key, start, end):
    return fetch_metars(list(stations_key), start, end)

@st.cache_data(ttl=120, show_spinner=False)
def _fetch_nws(station_id):
    return get_current_conditions(station_id)

@st.cache_data(ttl=180, show_spinner=False)
def _scan(station_ids, hours, cache_key):
    end = datetime.fromisoformat(cache_key).replace(tzinfo=timezone.utc)
    meta = {s["id"]: s for s in AOMC_STATIONS}
    return build_watchlist(
        [meta.get(sid, {"id": sid}) for sid in station_ids],
        hours=hours, end=end,
    )

STATUS_EMOJI = {
    "FLAGGED": "●", "INTERMITTENT": "◐", "RECOVERED": "○",
    "MISSING": "✖", "CLEAN": "✓", "NO DATA": "—",
}


def _render(builder, **kw):
    tmp = Path(f".tmp_{datetime.now().timestamp()}.png")
    try:
        builder(out_path=tmp, **kw)
        return tmp.read_bytes()
    finally:
        tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ASOS Tools",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Minimal CSS — only what Streamlit can't do natively.
st.markdown("""<style>
section.main > div.block-container { padding-top: 1.8rem; max-width: 1440px; }
[data-testid="stSidebar"] > div { padding-top: 0.8rem; }
div[data-testid="stMetric"] { background: #1e293b; padding: 0.7rem 1rem;
    border-radius: 6px; border-left: 3px solid #0ea5e9; }
</style>""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title("ASOS Tools")
with col_refresh:
    if st.button("Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.caption(
    "Automated Surface Observing System — network monitoring for "
    f"{len(AOMC_STATIONS)} federal stations. "
    "Data: NOAA/NCEI via Iowa Environmental Mesonet."
)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_summary, tab_reports, tab_stations, tab_flags, tab_missing = st.tabs([
    "Summary", "Reports", "Stations", "$ Flags", "Missing METARs",
])


# ---------------------------------------------------------------------------
# Shared: scan controls
# ---------------------------------------------------------------------------

def _scan_ui(key):
    """Render scope + window selectors; return (ids, hours, cache_key)."""
    c1, c2 = st.columns(2)
    with c1:
        hours = st.selectbox(
            "Window", [1, 2, 4, 6, 12, 24], index=2,
            format_func=lambda h: f"Last {h}h", key=f"{key}_h")
    with c2:
        scope = st.selectbox(
            "Scope",
            ["All AOMC (~920)", "By state", "Preset group"],
            key=f"{key}_sc")
    if scope == "By state":
        states = sorted({s.get("state") for s in AOMC_STATIONS if s.get("state")})
        pick = st.selectbox("State", states, key=f"{key}_st")
        ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("state") == pick)
    elif scope == "Preset group":
        grp = st.selectbox("Group", list_groups(),
                           format_func=lambda s: s.replace("_", " ").title(),
                           key=f"{key}_grp")
        ids = tuple(sid for sid in get_group(grp) if sid in AOMC_IDS) or tuple(get_group(grp))
    else:
        ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
    ck = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")
    return ids, hours, ck


def _status_df(wl, statuses):
    """Filter + format a watchlist slice for display."""
    df = wl[wl["status"].isin(statuses)].copy()
    if df.empty:
        return df
    df["Status"] = df["status"].map(lambda s: f"{STATUS_EMOJI.get(s, '')} {s}")
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
        st.error("AOMC catalog not loaded.")
        st.stop()

    all_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
    ck = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")

    with st.spinner("Scanning network…"):
        wl = _scan(all_ids, 4.0, ck)

    if wl.empty:
        st.warning("No data returned.")
    else:
        cts = wl["status"].value_counts()
        nf = int(cts.get("FLAGGED", 0))
        ni = int(cts.get("INTERMITTENT", 0))
        nr = int(cts.get("RECOVERED", 0))
        nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
        nc = int(cts.get("CLEAN", 0))

        # KPI row
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Clean", nc)
        c2.metric("Flagged ($)", nf)
        c3.metric("Missing", nm)
        c4.metric("Recovered", nr)
        c5.metric("Intermittent", ni)

        st.caption(f"Last 4 hours · {len(wl)} stations · scanned {datetime.now(timezone.utc):%H:%M:%S UTC}")

        st.divider()

        # Status message
        if nf == 0 and nm == 0:
            st.success(f"Network healthy. All {nc} stations reporting clean.")
        else:
            msg = []
            if nf: msg.append(f"**{nf}** flagged ($)")
            if ni: msg.append(f"**{ni}** intermittent")
            if nm: msg.append(f"**{nm}** missing METARs")
            if nr: msg.append(f"**{nr}** recovered")
            st.warning(" · ".join(msg) + f" · **{nc}** clean (not listed)")

        # Flagged table
        flagged = _status_df(wl, ["FLAGGED"])
        if not flagged.empty:
            st.subheader(f"Flagged Stations ({len(flagged)})")
            st.dataframe(
                flagged[["station", "name", "state", "probable_reason",
                         "flagged", "total", "flag_rate"]].rename(columns={
                    "station": "Station", "name": "Name", "state": "ST",
                    "probable_reason": "Probable Reason",
                    "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                }),
                use_container_width=True, hide_index=True,
                height=min(36 * len(flagged) + 38, 500),
                column_config={
                    "Rate %": st.column_config.ProgressColumn(
                        min_value=0, max_value=100, format="%.0f%%"),
                })

        # Intermittent
        inter = _status_df(wl, ["INTERMITTENT"])
        if not inter.empty:
            st.subheader(f"Intermittent ({len(inter)})")
            st.dataframe(
                inter[["station", "name", "state", "probable_reason"]].rename(columns={
                    "station": "Station", "name": "Name", "state": "ST",
                    "probable_reason": "Probable Reason",
                }),
                use_container_width=True, hide_index=True,
                height=min(36 * len(inter) + 38, 250))

        # Recovered
        recov = _status_df(wl, ["RECOVERED"])
        if not recov.empty:
            st.subheader(f"Recovered ({len(recov)})")
            st.dataframe(
                recov[["station", "name", "state", "minutes_since_last_flag"]].rename(columns={
                    "station": "Station", "name": "Name", "state": "ST",
                    "minutes_since_last_flag": "Min since $",
                }),
                use_container_width=True, hide_index=True,
                height=min(36 * len(recov) + 38, 250))

        # Missing
        miss = _status_df(wl, ["MISSING", "NO DATA"])
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
                height=min(36 * len(miss) + 38, 500))

        st.divider()
        st.caption(
            f"**{nc} stations** are reporting on schedule with no flags and no gaps. "
            "Not listed. Use the other tabs for detailed reports and station browsing."
        )


# ===========================================================================
# REPORTS
# ===========================================================================

with tab_reports:
    st.subheader("Generate Report")

    with st.sidebar:
        st.header("Report Settings")

        report_type = st.radio(
            "Report type",
            ["1-minute dashboard", "Maintenance flags ($)",
             "Flagged vs clean", "Missing METARs"],
        )

        st.subheader("Station(s)")
        smode = st.radio("Select by", ["Single", "Group", "Custom"],
                         horizontal=True)

        stations_list = []
        group_label = ""

        if smode == "Single":
            aomc_only = st.toggle("AOMC only", value=True) if _HAVE_AOMC else False
            if _HAVE_CATALOG:
                pool = ([s for s in ALL_ASOS_STATIONS if s["id"] in AOMC_IDS]
                        if aomc_only else ALL_ASOS_STATIONS)
                ids = [s["id"] for s in pool]
                sid = st.selectbox(
                    "Station", ids,
                    index=ids.index("KJFK") if "KJFK" in ids else 0,
                    format_func=lambda s: next(
                        (f"{s} — {r['name']} ({r.get('state','')})"
                         for r in pool if r["id"] == s), s))
            else:
                sid = st.text_input("ICAO ID", "KJFK").strip().upper()
            stations_list = [sid]
            group_label = sid

            # NWS current conditions
            cond = _fetch_nws(sid)
            if cond:
                st.divider()
                st.caption(f"Current: {cond.get('description', '—')}")
                mc1, mc2 = st.columns(2)
                mc1.metric("Temp", f"{cond['temp_f']}°F" if cond.get('temp_f') is not None else "—")
                mc2.metric("Wind",
                           f"{cond.get('wind_speed_kt', 0):.0f}kt"
                           if cond.get('wind_speed_kt') is not None else "—")

        elif smode == "Group":
            grp = st.selectbox("Group", list_groups(),
                               format_func=lambda s: s.replace("_", " ").title())
            stations_list = list(get_group(grp))
            group_label = grp.replace("_", " ").title()
        else:
            raw = st.text_area("Station IDs", "KJFK\nKLGA\nKEWR")
            stations_list = [s.strip().upper() for s in raw.replace(",", "\n").splitlines() if s.strip()]
            group_label = ", ".join(stations_list[:3]) + ("…" if len(stations_list) > 3 else "")

        st.caption(f"{len(stations_list)} station(s)")

        st.subheader("Time Window")
        wmode = st.radio("Window", ["1 day", "7 days", "14 days", "30 days", "Custom"],
                         index=1)
        if wmode == "Custom":
            today = datetime.now(timezone.utc).date()
            sd = st.date_input("Start", today - timedelta(days=7))
            ed = st.date_input("End", today)
            start = datetime.combine(sd, datetime.min.time(), tzinfo=timezone.utc)
            end = datetime.combine(ed, datetime.min.time(), tzinfo=timezone.utc)
            wlabel = f"{(end - start).days} day"
        else:
            days = {"1 day": 1, "7 days": 7, "14 days": 14, "30 days": 30}[wmode]
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=days)
            wlabel = f"{days} day"

        go = st.button("Generate", type="primary", use_container_width=True)

    if not go:
        st.info("Configure report settings in the sidebar and click **Generate**.")
    else:
        try:
            if report_type == "1-minute dashboard":
                stn = stations_list[0]
                with st.spinner(f"Fetching {stn}…"):
                    df = _fetch_1min(stn, start, end)
                if df.empty:
                    st.error("No data.")
                else:
                    name = str(df["station_name"].iloc[0])
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Station", stn)
                    c2.metric("Name", name)
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
            else:
                with st.spinner("Fetching METARs…"):
                    metars = _fetch_metars(tuple(stations_list), start, end)
                if metars.empty:
                    st.error("No METARs.")
                else:
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Stations", metars["station"].nunique())
                    c2.metric("METARs", f"{len(metars):,}")
                    nflag = int(metars["has_maintenance"].sum())
                    c3.metric("Flagged", f"{nflag} ({nflag/len(metars)*100:.0f}%)")

                    builders = {
                        "Maintenance flags ($)": (build_maintenance_report, "maintenance"),
                        "Flagged vs clean": (build_comparison_report, "comparison"),
                        "Missing METARs": (build_missing_report, "missing"),
                    }
                    builder, kind = builders[report_type]
                    with st.spinner("Rendering…"):
                        png = _render(builder, metars_df=metars,
                                      group_label=group_label, window_label=wlabel)
                    st.image(png, use_container_width=True)

                    c1, c2 = st.columns(2)
                    slug = group_label.lower().replace(" ", "-").replace(",", "")
                    c1.download_button("Download PNG", png,
                                       f"{slug}_{wlabel.replace(' ', '')}_{kind}.png",
                                       "image/png", use_container_width=True)
                    c2.download_button("Download CSV",
                                       metars.to_csv(index=False).encode(),
                                       f"{slug}_{wlabel.replace(' ', '')}_metars.csv",
                                       "text/csv", use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
            st.exception(e)


# ===========================================================================
# STATIONS
# ===========================================================================

with tab_stations:
    st.subheader("AOMC Federal Station Directory")
    st.caption(
        f"{len(AOMC_STATIONS)} stations from NCEI HOMR — "
        "the authoritative list of NWS / FAA / DOD operated ASOS sites."
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

        df = pd.DataFrame([{
            "ICAO": s.get("id"), "Call": s.get("call"),
            "Name": s.get("name"), "State": s.get("state"),
            "County": s.get("county"),
            "Lat": s.get("lat"), "Lon": s.get("lon"),
            "Elev (ft)": s.get("elev_ft"),
            "WBAN": s.get("wban"), "Types": s.get("station_types"),
        } for s in rows])

        st.caption(f"{len(rows)} stations match.")
        st.dataframe(df, use_container_width=True, height=600, hide_index=True)

        if rows:
            st.download_button("Download CSV", df.to_csv(index=False).encode(),
                               "aomc_stations.csv", "text/csv")
    else:
        st.error("AOMC catalog not loaded.")


# ===========================================================================
# $ FLAGS
# ===========================================================================

with tab_flags:
    st.subheader("$ Maintenance Flag Watchlist")
    st.caption(
        "Stations currently reporting the $ maintenance-check indicator. "
        "The $ flag means the ASOS self-test detected an out-of-tolerance "
        "condition — it does not mean the data is inaccurate."
    )

    if _HAVE_AOMC:
        ids, hours, ck = _scan_ui("fl")
        st.caption(f"Scanning {len(ids)} stations, last {hours}h…")

        with st.spinner("Scanning…"):
            wl = _scan(ids, float(hours), ck)

        if wl.empty:
            st.warning("No data.")
        else:
            cts = wl["status"].value_counts()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Flagged", int(cts.get("FLAGGED", 0)))
            c2.metric("Intermittent", int(cts.get("INTERMITTENT", 0)))
            c3.metric("Recovered", int(cts.get("RECOVERED", 0)))
            c4.metric("Clean", int(cts.get("CLEAN", 0)))

            st.caption(f"Scanned {datetime.now(timezone.utc):%H:%M:%S UTC}")

            show_clean = st.checkbox("Include clean stations", key="fl_clean")
            keep = ["FLAGGED", "INTERMITTENT", "RECOVERED"]
            if show_clean:
                keep.append("CLEAN")
            view = _status_df(wl, keep)

            if not view.empty:
                st.dataframe(
                    view[["station", "name", "state", "Status",
                          "probable_reason", "flagged", "total", "flag_rate",
                          "latest_time", "latest_flag_time"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "probable_reason": "Reason",
                        "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                        "latest_time": "Latest", "latest_flag_time": "Last $",
                    }),
                    use_container_width=True, height=560, hide_index=True,
                    column_config={
                        "Rate %": st.column_config.ProgressColumn(
                            min_value=0, max_value=100, format="%.0f%%"),
                    })
                st.download_button("Download CSV", view.to_csv(index=False).encode(),
                                   f"flags_{hours}h.csv", "text/csv")

            with st.expander("What does $ mean?"):
                st.markdown("""
The `$` maintenance-check indicator is appended by the ASOS when its
internal self-test detects an out-of-tolerance condition. Specific
sensor codes in the METAR remarks (if present) are decoded:

| Code | Sensor |
|---|---|
| RVRNO | Runway Visual Range |
| PWINO | Precipitation identification |
| PNO | Precipitation amount |
| FZRANO | Freezing rain detection |
| TSNO | Lightning / thunderstorm |
| VISNO | Visibility (secondary) |
| CHINO | Cloud height (secondary) |

Most `$` flags show "Internal check" — the ASOS found a tolerance
drift but no specific sensor code is in the remarks.
                """)
    else:
        st.error("AOMC catalog not loaded.")


# ===========================================================================
# MISSING METARS
# ===========================================================================

with tab_missing:
    st.subheader("Missing METAR Monitor")
    st.caption(
        "Stations that missed scheduled hourly METARs. ASOS routine: "
        "one report per hour at ~HH:51Z. A silent station is more "
        "critical than a flagged one — no data at all."
    )

    if _HAVE_AOMC:
        ids, hours, ck = _scan_ui("ms")
        st.caption(f"Scanning {len(ids)} stations, last {hours}h…")

        with st.spinner("Scanning…"):
            wl = _scan(ids, float(hours), ck)

        if wl.empty:
            st.warning("No data.")
        else:
            cts = wl["status"].value_counts()
            nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
            nr = len(wl) - nm

            c1, c2, c3 = st.columns(3)
            c1.metric("Missing", nm)
            c2.metric("Reporting", nr)
            c3.metric("Total", len(wl))

            st.caption(f"Scanned {datetime.now(timezone.utc):%H:%M:%S UTC}")

            show_ok = st.checkbox("Include reporting stations", key="ms_ok")
            if show_ok:
                view = _status_df(wl, list(wl["status"].unique()))
            else:
                view = _status_df(wl, ["MISSING", "NO DATA"])

            if not view.empty:
                st.dataframe(
                    view[["station", "name", "state", "Status",
                          "missing", "expected_hourly", "missing_hours_utc",
                          "minutes_since_last_report"]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "missing": "Gaps", "expected_hourly": "Expected",
                        "missing_hours_utc": "Missing Hours",
                        "minutes_since_last_report": "Min since report",
                    }),
                    use_container_width=True, height=560, hide_index=True)
                st.download_button("Download CSV", view.to_csv(index=False).encode(),
                                   f"missing_{hours}h.csv", "text/csv")

            with st.expander("Why do METARs go missing?"):
                st.markdown("""
| Cause | Notes |
|---|---|
| Power outage | Station lost electricity |
| Comm failure | ASOS running but METAR can't reach NWS network |
| Decommissioned | Seasonal or permanently offline |
| Sensor cascade | All sensors fail; ASOS decides not to file |
| IEM lag | Rare — ingestion delay from NCEI |
                """)
    else:
        st.error("AOMC catalog not loaded.")


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    "ASOS Tools · "
    "[Source](https://github.com/consigcody94/asos-tools-py) · "
    "Data: NOAA/NCEI via [IEM](https://mesonet.agron.iastate.edu) · "
    "Catalog: [NCEI HOMR](https://www.ncei.noaa.gov/access/homr/)"
)
