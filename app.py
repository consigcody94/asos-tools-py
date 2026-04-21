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
from asos_tools.incident_docx import generate_incident_docx
from asos_tools.nws import get_current_conditions
from asos_tools.stations import GROUPS, get_group, list_groups
from asos_tools.watchlist import build_watchlist

# --- Tier 1 + Tier 3 integrations ---
try:
    from streamlit_folium import st_folium
    from asos_tools.map_view import build_status_map, STATUS_COLORS
    _HAVE_FOLIUM = True
except ImportError:
    _HAVE_FOLIUM = False

# --- Release #2: 3D globe (Globe.gl / three.js) ---
try:
    from asos_tools.globe_view import build_globe_html
    _HAVE_GLOBE = True
except ImportError:
    _HAVE_GLOBE = False

try:
    from asos_tools.news import fetch_noaa_faa_headlines
    _HAVE_NEWS = True
except ImportError:
    _HAVE_NEWS = False

try:
    from st_aggrid import (
        AgGrid, GridOptionsBuilder, GridUpdateMode,
        ColumnsAutoSizeMode, JsCode,
    )
    _HAVE_AGGRID = True
except ImportError:
    _HAVE_AGGRID = False

try:
    from asos_tools import alerts as owl_alerts
    _HAVE_APPRISE = True
except ImportError:
    _HAVE_APPRISE = False

try:
    from asos_tools.pdf_export import build_watchlist_pdf, build_report_pdf
    _HAVE_PDF = True
except ImportError:
    _HAVE_PDF = False

# Auth module intentionally not imported — app is public.
_HAVE_AUTH = False

try:
    from asos_tools.scheduler import (
        get_scheduler, schedule_watchlist_refresh, scheduler_status,
    )
    _HAVE_SCHED = True
except ImportError:
    _HAVE_SCHED = False

try:
    from asos_tools.persistent_cache import (
        put_watchlist as _pc_put, get_watchlist as _pc_get,
        cache_stats as _pc_stats, clear_cache as _pc_clear,
    )
    _HAVE_PC = True
except ImportError:
    _HAVE_PC = False

try:
    from asos_tools.anomaly import detect_anomalies
    _HAVE_ANOMALY = True
except ImportError:
    _HAVE_ANOMALY = False

# --- Release #1 new data sources ---
try:
    from asos_tools import awc as owl_awc
    _HAVE_AWC = True
except ImportError:
    _HAVE_AWC = False

try:
    from asos_tools import alerts_feed as owl_caps
    _HAVE_CAPS = True
except ImportError:
    _HAVE_CAPS = False

try:
    from asos_tools import news as owl_news
    _HAVE_NEWS = True
except ImportError:
    _HAVE_NEWS = False

try:
    from asos_tools.sources import SOURCES as DATA_SOURCES
    _HAVE_SOURCES = True
except ImportError:
    DATA_SOURCES = []
    _HAVE_SOURCES = False

try:
    from asos_tools import webcams as owl_webcams
    _HAVE_WEBCAMS = True
except ImportError:
    _HAVE_WEBCAMS = False

try:
    from asos_tools.ncei import fetch_metars_ncei, service_available as _ncei_avail
    _HAVE_NCEI = True
except ImportError:
    _HAVE_NCEI = False

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
    """Round to nearest 3-min boundary — kept for compatibility with
    the scheduler's cache-key format, NOT used as a live cache key.

    Live cache keys come from :func:`_session_data_key` which only
    changes when the user hits the Refresh button.
    """
    return dt.replace(second=0, microsecond=0,
                      minute=(dt.minute // 3) * 3).isoformat()


def _session_data_key() -> str:
    """Return the cache key for all upstream data lookups this session.

    The key is set on first render of the session (using the current UTC
    time), then stays fixed until the user clicks Refresh — at which
    point we bump the key AND call ``st.cache_data.clear()``. Because
    ``@st.cache_data`` is keyed by positional args, a new key value
    forces a fresh fetch without relying on time-based expiration.
    """
    if "_data_key" not in st.session_state:
        st.session_state["_data_key"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return st.session_state["_data_key"]


def _bump_data_key() -> None:
    """Invalidate all cached fetches — called by the Refresh button."""
    st.session_state["_data_key"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    st.cache_data.clear()


def _round_5min(dt: datetime) -> datetime:
    """Round to nearest 5-min boundary for stable fetch cache keys."""
    return dt.replace(second=0, microsecond=0,
                      minute=(dt.minute // 5) * 5)


@st.cache_data(ttl=None, show_spinner=False)
def _cached_news(ck: str = "") -> list[dict]:
    """Cached news headlines for the globe ticker.

    Keyed on the session's data-key so a Refresh click invalidates.
    Returns up to 20 items; swallows individual feed failures.
    """
    if not _HAVE_NEWS:
        return []
    try:
        return owl_news.fetch_noaa_faa_headlines(limit=20, sort="time") or []
    except Exception:
        logger.exception("news fetch failed")
        return []


def _section_help(
    title: str,
    *,
    what: str,
    who: list[str] | None = None,
    how: list[str] | None = None,
    output: str | None = None,
    expanded: bool = False,
) -> None:
    """Render a standard "help + who uses it + how to use" expander.

    Used at the top of every tab / major section so that both experts
    (who skip it) and new users (who expand it) can understand what the
    section does and how to drive it.  Keeping a single helper means
    visual style stays consistent across the whole app.
    """
    with st.expander(title, expanded=expanded):
        out = ["**What it is.**  " + what.strip()]
        if who:
            out.append("\n**Who uses it:**\n")
            for item in who:
                out.append(f"- {item}")
        if how:
            out.append("\n**How to use it:**\n")
            for i, step in enumerate(how, 1):
                out.append(f"{i}. {step}")
        if output:
            out.append("\n**What the output means.**  " + output.strip())
        st.markdown("\n".join(out), unsafe_allow_html=False)


def _render_status_glossary(key_suffix: str = "", expanded: bool = False) -> None:
    """Collapsible legend defining every status enum the watchlist uses.

    Call this anywhere a user sees status badges — Summary tab,
    AOMC Controllers tab, Reports tab, etc. — so they can look up what
    "RECOVERED" or "INTERMITTENT" actually means without leaving the page.

    Parameters
    ----------
    key_suffix
        Unique suffix for this instance's Streamlit widget key, so
        repeated expanders on the same page don't collide.
    expanded
        Whether to open the expander by default.
    """
    with st.expander("Status definitions", expanded=expanded):
        st.markdown(
            """
Every ASOS station in the watchlist is classified into exactly one of
these six states for the scan window (default: last 4 hours). Order
below is worst -> best; the Stations Requiring Attention table sorts
the same way.

| Status | Meaning | What to do |
|---|---|---|
| **MISSING** (red) | At least one scheduled hourly METAR did not arrive. The station may be silent — worse than flagged because you have no data at all. | Verify comms / power / the ASOS controller. Check NOTAMs for planned outages. |
| **FLAGGED** (amber) | The most recent METAR ended with `$`, the ASOS self-diagnostic indicator. A sensor is out-of-tolerance *right now*. | Decode `$` remarks (RVRNO, PWINO, etc.) to identify which sensor. Open a maintenance ticket. |
| **INTERMITTENT** (yellow) | METARs in the window mix flagged and clean, and the *latest* report is flagged. The sensor is oscillating in and out of tolerance — unstable. | Same root causes as FLAGGED. Often the last stage before a total sensor failure. |
| **RECOVERED** (cyan) | Station *was* flagged earlier in the window, but the **last two METARs are clean**. The sensor is back online. | Verify the fix persisted (watch through the next scheduled report). If possible, log what was done so the incident is traceable. |
| **CLEAN** (green) | Zero `$` flags AND zero missing hours in the window. Nominal operation. | Nothing. |
| **NO DATA** (slate) | IEM returned zero observations for this station within the window. Usually means the station has been offline for weeks, is decommissioned, or is in a remote area with sparse reporting. | If unexpected, escalate — this is rarer than MISSING and typically points to a long-running problem. |

---

**Scan window & timing:**

- Default window = the last **4 hours** (configurable on AOMC Controllers tab).
- ASOS routine METARs are scheduled once per hour, typically at HH:51Z.
- A "missing hour" = zero METARs between the top of that hour and the next.
- SPECI (special) reports filed at HH:47 or later count toward the *next* scheduled hour, which is why you occasionally see a single late-filed METAR satisfy two adjacent buckets.

**Worst-first ordering** in the attention table:

`MISSING > FLAGGED > INTERMITTENT > RECOVERED > CLEAN > NO DATA`

Within each status, rows are sorted by severity (more missing hours first, then higher flag rate first, then alphabetical by station ID).
            """,
            unsafe_allow_html=False,
        )


def _html_escape(s: str) -> str:
    """HTML-escape a string for safe interpolation into raw-HTML blocks.

    Streamlit's ``st.markdown(..., unsafe_allow_html=True)`` does not
    escape interpolated values — a crafted METAR remark or station name
    could otherwise inject ``<script>``.  Always pass user-or-METAR-
    derived fields through this before substituting into HTML strings.
    """
    import html as _html
    return _html.escape(str(s or ""), quote=True)


def _render_drill_panel(sid: str, plk: dict, wl) -> None:
    """Render the station-detail panel below the globe.

    Shows: status badge + name/state, latest METAR, nearest FAA webcams
    (up to 4 thumbnails that auto-link to the FAA portal), and any
    active NWS CAP alerts for the station's state.
    """
    station = plk.get(sid) or {}
    name = (station.get("name") or "").title()
    state = station.get("state") or ""
    lat = station.get("lat")
    lon = station.get("lon")

    # Status from the watchlist row, if present.
    status = "NO DATA"
    latest_metar = ""
    probable_reason = ""
    try:
        row = wl[wl["station"] == sid]
        if not row.empty:
            status = str(row.iloc[0].get("status") or "NO DATA").upper()
            latest_metar = str(row.iloc[0].get("latest_metar") or "")
            probable_reason = str(row.iloc[0].get("probable_reason") or "")
    except Exception:
        pass

    # Status badge color (matches globe_view.STATUS_COLORS).
    badge_colors = {
        "MISSING":      "#dc2626",
        "FLAGGED":      "#f59e0b",
        "INTERMITTENT": "#eab308",
        "RECOVERED":    "#06b6d4",
        "CLEAN":        "#22c55e",
        "NO DATA":      "#64748b",
    }
    bc = badge_colors.get(status, "#64748b")

    # --- Header row --------------------------------------------------------
    # All interpolated values are HTML-escaped because METAR-derived text
    # (name, probable_reason, status) could contain < > & that would break
    # the surrounding markup or be exploited by a malicious remark string.
    _sid_e, _name_e, _state_e, _status_e = (
        _html_escape(sid), _html_escape(name),
        _html_escape(state), _html_escape(status),
    )
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:14px;
                    padding:10px 14px;
                    background:rgba(2,6,23,0.06);
                    border-left:4px solid {bc};
                    border-radius:4px;margin-top:8px;">
          <div style="font-family:'JetBrains Mono',ui-monospace,monospace;
                      font-size:20px;font-weight:600;color:#0f172a;">
            {_sid_e}
          </div>
          <div style="flex:1;color:#334155;">{_name_e} &middot; {_state_e}</div>
          <span style="background:{bc};color:#fff;padding:3px 10px;
                       border-radius:3px;font-size:11px;font-weight:600;
                       letter-spacing:0.08em;">{_status_e}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if probable_reason:
        st.caption(f"**Probable reason:** {probable_reason}")

    # --- METAR — full width (no column split above webcams) ---------------
    st.markdown("**Latest METAR**")
    if latest_metar:
        st.code(latest_metar, language=None)
    else:
        st.caption("No recent METAR available for this station.")

    # --- Sensor Health Grid (avwx-engine structured parse) ---------------
    # Decodes the METAR into per-sensor status; a quick visual summary of
    # which specific ASOS sensors are live, flagged, or unknown right now.
    if latest_metar:
        try:
            from asos_tools.metar_parse import parse_metar, sensor_health_grid
            info = parse_metar(latest_metar, station=sid)
            rows = sensor_health_grid(info)
        except Exception:
            logger.exception("sensor grid parse failed")
            info, rows = None, []

        if info and rows:
            fc = str(info.get("flight_category") or "-")
            fc_colors = {"VFR": "#22c55e", "MVFR": "#38bdf8",
                         "IFR": "#f59e0b", "LIFR": "#dc2626"}
            fc_color = fc_colors.get(fc, "#64748b")
            st.markdown(
                f"**Sensor Health Grid** &nbsp; "
                f"<span style='background:{fc_color};color:#fff;"
                f"padding:2px 8px;border-radius:3px;font-size:11px;"
                f"font-weight:700;letter-spacing:0.08em;'>"
                f"FLT CAT: {_html_escape(fc)}</span>",
                unsafe_allow_html=True,
            )
            _section_help(
                "About the Sensor Health Grid",
                what=(
                    "Decodes the current METAR into a per-sensor status "
                    "readout. Each of the ASOS's ~12 discrete sensors "
                    "(wind, temp/dew, altimeter, visibility, ceilometer, "
                    "precip gauge, present-weather ID, lightning, "
                    "freezing-rain detector, RVR, SLP, and the overall "
                    "`$` self-check) gets a green/red/grey chip based on "
                    "what avwx-engine parses from the obs. Green = value "
                    "present and plausible; Red = explicit failure code "
                    "in remarks (RVRNO, PWINO, TSNO, CHINO, etc.); Grey = "
                    "silent (either the sensor didn't report this cycle "
                    "or it's not applicable, e.g. no ceiling when clear)."
                ),
                who=[
                    "**AOMC controllers** — identify which specific sensor triggered the `$` flag directly from the grid, without parsing remark strings by hand.",
                    "**Field maintenance techs** — know what part to bring to the truck roll before you leave the shop.",
                    "**Incident investigators** — map a sensor failure to the exact START time when correlating against the Matrix Profile anomaly chart.",
                    "**Shift supervisors** — quick triage whether a flag is a drift (Temp/Dew red, everything else green) or a total comms failure (most cells red).",
                ],
                how=[
                    "Pick a station from the selectbox above; the grid updates from that station's latest METAR automatically.",
                    "Hover any red chip to see the exact remark code (e.g. `PWINO reported`).",
                    "Cross-reference with the `Latest METAR` block above to see the raw evidence.",
                    "If every chip is grey, the station is MISSING — see the Status definitions expander on the Summary tab for next steps.",
                ],
                output=(
                    "Sensor codes are the FAA-standard ASOS self-diagnostic "
                    "indicators: RVRNO = runway visual range out, PWINO = "
                    "present-weather identifier out, PNO = precip amount "
                    "gauge out, FZRANO = freezing-rain sensor out, TSNO = "
                    "lightning sensor out, SLPNO = sea-level pressure "
                    "calc unavailable, VISNO = prevailing vis sensor out, "
                    "CHINO = ceilometer out."
                ),
            )
            # Render as a 4-column compact grid of chips.
            COLS = 4
            col_groups = st.columns(COLS)
            for i, row in enumerate(rows):
                ok = row["ok"]
                if ok is True:
                    bg, fg, sym = "#dcfce7", "#14532d", "OK"
                elif ok is False:
                    bg, fg, sym = "#fee2e2", "#7f1d1d", "FAIL"
                else:
                    bg, fg, sym = "#e2e8f0", "#334155", "--"
                reason = row.get("reason", "")
                with col_groups[i % COLS]:
                    tip = (f' title="{_html_escape(reason)}"'
                           if reason else "")
                    st.markdown(
                        f"<div{tip} style='background:{bg};color:{fg};"
                        f"padding:6px 10px;border-radius:4px;"
                        f"font-size:11px;font-weight:600;"
                        f"margin-bottom:4px;display:flex;"
                        f"justify-content:space-between;'>"
                        f"<span>{_html_escape(row['sensor'])}</span>"
                        f"<span style='opacity:0.65;'>{sym}</span></div>",
                        unsafe_allow_html=True,
                    )

    # --- Webcams — single horizontal row, 4 thumbs side by side ----------
    # Stacking METAR above webcams (instead of splitting left/right)
    # eliminates the big grey dead-space we were seeing when one outer
    # column was much shorter than the other.
    st.markdown("**Nearest FAA WeatherCams** (within 25 NM)")
    if _HAVE_WEBCAMS and lat is not None and lon is not None:
        try:
            cams = owl_webcams.cameras_near(
                float(lat), float(lon), radius_nm=25.0, limit=4,
            )
        except Exception:
            logger.exception("webcam lookup failed")
            cams = []

        if cams:
            # One column per camera up to 4; Streamlit distributes width
            # evenly and each cell sizes to its own content naturally.
            cam_cols = st.columns(min(4, len(cams)))
            for i, cam in enumerate(cams[:4]):
                with cam_cols[i]:
                    try:
                        img_url = owl_webcams.latest_image_url(cam["id"])
                    except Exception:
                        img_url = None
                    if img_url:
                        st.image(img_url, use_container_width=True)
                    st.caption(
                        f"**{cam.get('site_name','')}** · "
                        f"{cam.get('direction','')} · "
                        f"{cam.get('distance_nm','?')} NM"
                    )
        else:
            st.caption("No FAA WeatherCams within 25 NM of this station.")
    else:
        st.caption(
            "Webcam lookup unavailable — either FAA API is unreachable "
            "or this station has no known lat/lon."
        )

    # --- CAP alerts (active) ----------------------------------------------
    if _HAVE_CAPS and state:
        st.markdown("**Active NWS CAP Alerts** for this region")
        try:
            alerts = owl_caps.alerts_for_state(state)[:5]
        except Exception:
            logger.exception("CAP fetch failed")
            alerts = []
        if alerts:
            for a in alerts:
                sev = a.get("severity", "Unknown")
                event = a.get("event") or "Alert"
                headline = a.get("headline") or ""
                st.markdown(f"- **{event}** ({sev}) — {headline}")
        else:
            st.caption("No active CAP alerts for this state.")


def _wlabel(days: int) -> str:
    return f"{days} day{'s' if days != 1 else ''}"


# ---------------------------------------------------------------------------
# Cached fetches — FETCH-ON-DEMAND MODEL
# ---------------------------------------------------------------------------
# All upstream data fetches use ``ttl=None`` (no time-based expiration).
# The cache only invalidates when the user clicks the sidebar "Refresh"
# button (which calls ``st.cache_data.clear()``), or when cache-key inputs
# change (e.g., a different station or time window).
#
# This means:
#   - Page loads: data fetched ONCE per session, then served from cache.
#   - Streamlit reruns (tab switch, widget click): cache hit, no network.
#   - User explicit refresh: cache cleared, next render re-fetches.
# No background polling, no timed expiration.

@st.cache_data(ttl=None, show_spinner=False)
def _fetch_1min(station, start, end):
    return fetch_1min(station, start, end)

@st.cache_data(ttl=None, show_spinner=False)
def _fetch_metars(stations_key, start, end):
    return fetch_metars(list(stations_key), start, end)

@st.cache_data(ttl=None, show_spinner=False)
def _fetch_nws(station_id):
    # Security: validate station ID before passing to NWS URL path.
    if not re.fullmatch(r"[A-Z0-9]{3,6}", station_id.strip().upper()):
        return None
    return get_current_conditions(station_id)

@st.cache_data(ttl=None, show_spinner=False)
def _scan(station_ids, hours, cache_key):
    """Watchlist scan. cache_key comes from session-scoped _scan_key()."""
    end = datetime.fromisoformat(cache_key).replace(tzinfo=timezone.utc)
    return build_watchlist(
        [_AOMC_META.get(sid, {"id": sid}) for sid in station_ids],
        hours=hours, end=end,
    )


# ---- Cached wrappers for Release #1 data sources (all session-long) ----
@st.cache_data(ttl=None, show_spinner=False)
def _awc_airsigmet(_cache_key: str):
    if not _HAVE_AWC:
        return []
    return owl_awc.fetch_airsigmet()

@st.cache_data(ttl=None, show_spinner=False)
def _awc_pirep(_cache_key: str):
    if not _HAVE_AWC:
        return []
    return owl_awc.fetch_pirep(age_hours=2)

@st.cache_data(ttl=None, show_spinner=False)
def _awc_metar_bulk(ids_tuple, _cache_key: str):
    if not _HAVE_AWC:
        return []
    return owl_awc.fetch_metar(list(ids_tuple))

@st.cache_data(ttl=None, show_spinner=False)
def _awc_taf(station_id: str, _cache_key: str):
    if not _HAVE_AWC:
        return []
    return owl_awc.fetch_taf([station_id])

@st.cache_data(ttl=None, show_spinner=False)
def _caps_active(_cache_key: str):
    if not _HAVE_CAPS:
        return []
    return owl_caps.fetch_active_alerts()

@st.cache_data(ttl=None, show_spinner=False)
def _news_headlines(_cache_key: str, limit: int = 30):
    if not _HAVE_NEWS:
        return []
    return owl_news.fetch_noaa_faa_headlines(limit=limit)


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
    # Use the real OWL logo PNG as the browser tab favicon instead of a
    # Unicode glyph (which rendered as a tofu box in locked-down corp
    # browsers + gave the app a hobbyist aesthetic).
    page_icon="owl_logo.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""<style>
/* Professional typography: Inter for UI, Space Grotesk for headings */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;600;700&family=JetBrains+Mono:wght@400;500&family=Public+Sans:wght@400;500;600;700&display=swap');
/* Material Symbols font — Streamlit uses it internally for dropdown
   chevrons, sort arrows, tab scroll buttons, checkboxes, etc.
   Loading it explicitly so the ligature names ("check", "arrow_drop_down")
   don't render as literal text. */
@import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,300..700,0..1,-50..200&family=Material+Symbols+Rounded&family=Material+Icons&display=swap');

/* Apply Inter only to body-level and markdown-container elements.
   We deliberately DON'T use a wildcard like [class*="st-"] because that
   steamrolls Streamlit's internal icon spans (<span
   data-testid="stExpanderIconCheck">check</span> etc.) and forces them to
   render the Material ligature name as Inter text. */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMarkdownContainer"],
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] td,
[data-testid="stMarkdownContainer"] th,
[data-testid="stCaptionContainer"],
.stButton button,
.stDownloadButton button,
.stSelectbox label, .stMultiSelect label,
.stTextInput label, .stTextArea label,
.stNumberInput label, .stDateInput label,
.stRadio label, .stCheckbox label, .stSlider label,
.stTabs [data-baseweb="tab"],
input, select, textarea {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif !important;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    font-feature-settings: 'cv11', 'ss01', 'ss03';
}

/* Nail the Material font on every icon container Streamlit uses.
   High specificity wins + belt-and-suspenders liga hints. */
[data-testid*="Icon"],
[data-testid*="icon"],
[data-baseweb="icon"],
.material-icons,
.material-symbols-outlined,
.material-symbols-rounded,
span.material-symbols-outlined,
span[class*="material-symbols"] {
    font-family: 'Material Symbols Outlined', 'Material Symbols Rounded',
                 'Material Icons' !important;
    font-feature-settings: 'liga' !important;
    font-display: block !important;
    letter-spacing: normal !important;
    /* The ligature names are ASCII words; keep them intact for lookup. */
    text-transform: none !important;
    white-space: nowrap !important;
    direction: ltr !important;
    -webkit-font-feature-settings: 'liga' !important;
    -moz-font-feature-settings: 'liga' !important;
    -webkit-font-smoothing: antialiased !important;
    text-rendering: optimizeLegibility !important;
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

/* Tabs — clean underline style + sticky along top of viewport */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    position: sticky;
    top: 0;
    z-index: 50;
    background: var(--owl-tab-bg, #ffffff);
    backdrop-filter: blur(6px);
    border-bottom: 1px solid rgba(0, 51, 102, 0.08);
    padding-top: 4px;
}
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

/* Mobile: reduce tab padding + horizontally scroll if crowded */
@media (max-width: 760px) {
    .stTabs [data-baseweb="tab-list"] {
        overflow-x: auto;
        flex-wrap: nowrap;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 0.55rem 0.75rem !important;
        font-size: 0.82rem !important;
        white-space: nowrap;
    }
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

/* Main content image (reports) — reserve space to prevent page jump.
   Applies to every st.image in the main area regardless of wrapper. */
section.main [data-testid="stImage"],
section.main div[data-testid="stImage"],
[data-testid="stVerticalBlock"] [data-testid="stImage"] {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid rgba(128,128,128,0.12);
    min-height: 780px !important;
    background: #f8fafc;
    display: block;
}
section.main [data-testid="stImage"] img,
[data-testid="stVerticalBlock"] [data-testid="stImage"] img {
    width: 100% !important;
    height: auto !important;
    display: block;
}
/* Sidebar image (logo) — override the main-area sizing */
[data-testid="stSidebar"] [data-testid="stImage"] {
    min-height: 0 !important;
    border: none !important;
    background: transparent !important;
}

/* Disable smooth scroll so Streamlit can't animate scroll position
   when content is injected into a placeholder. */
html, body, .main, section.main { scroll-behavior: auto !important; }

/* Report placeholder: reserve vertical space BEFORE the image loads
   so the page doesn't jump when st.image() fills the slot. */
.stSpinner { min-height: 40px; }

/* === USWDS-STYLE FEDERAL BANNER === */
/* Sits above the ops banner.  GSA-style "official website" strip that
   reads as government the first second the page loads. */
.usa-banner {
    background: #f0f0f0;
    border-bottom: 1px solid #d0d0d0;
    padding: 4px 0;
    font-family: 'Public Sans', 'Source Sans Pro', 'Inter', sans-serif;
    font-size: 11px;
    color: #1b1b1b;
    margin: -16px -16px 0 -16px;
}
.usa-banner-inner {
    max-width: 1460px;
    margin: 0 auto;
    padding: 0 16px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.usa-banner-flag {
    font-size: 13px;
    line-height: 1;
}
.usa-banner-text {
    letter-spacing: 0.01em;
    font-weight: 500;
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
    font-family: 'Space Grotesk', 'Public Sans', sans-serif;
    font-size: 3.4rem;
    font-weight: 800;
    letter-spacing: -0.035em;
    color: #003366;
    line-height: 1;
    font-feature-settings: 'tnum', 'lnum';
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
        ck = _session_data_key()
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
    st.markdown(
        '<div style="font-size:0.72rem;color:#64748b;line-height:1.45;">'
        'Pick a workflow from the tabs above. Different NOAA groups '
        '(AOMC controllers, forecasters, administrators) each have '
        'their own section.</div>',
        unsafe_allow_html=True,
    )

    # ---- Sidebar footer (refresh + theme) ----
    st.divider()
    # Show when data was last loaded (session key == load time).
    _loaded_iso = st.session_state.get("_data_key", "—")
    try:
        _loaded_dt = datetime.fromisoformat(_loaded_iso).replace(tzinfo=timezone.utc)
        _loaded_display = _loaded_dt.strftime("%Y-%m-%d %H:%M:%SZ")
    except Exception:
        _loaded_display = _loaded_iso
    st.caption(f"Data loaded: **{_loaded_display}**")

    fc1, fc2 = st.columns(2)
    with fc1:
        if st.button("Refresh", use_container_width=True,
                     help="Clear cached data and re-fetch from upstream sources."):
            _bump_data_key()
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

        /* Tabs — sticky bar keeps the dark surface under the blur */
        .stTabs [data-baseweb="tab-list"] {
            border-bottom: 1px solid #1f2a44 !important;
            background: rgba(10, 16, 32, 0.88) !important;
        }
        .stTabs [data-baseweb="tab"] { color: #94a3b8 !important; }
        .stTabs [data-baseweb="tab"]:hover { color: #cbd5e1 !important; }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            color: #f1f5f9 !important;
            border-bottom-color: #38bdf8 !important;
        }

        /* Dark-mode widget audit — every input the UI exposes. */
        .stDateInput input, .stTimeInput input,
        .stNumberInput input, .stTextInput input, .stTextArea textarea {
            background: #0f1730 !important;
            color: #e2e8f0 !important;
            border-color: #2a3a5c !important;
        }
        .stDateInput input::placeholder,
        .stTextInput input::placeholder { color: #64748b !important; }
        .stRadio > div, .stCheckbox > label {
            color: #e2e8f0 !important;
        }
        .stSlider [data-baseweb="slider"] div[role="slider"] {
            background: #38bdf8 !important;
            border-color: #38bdf8 !important;
        }
        /* Date picker popover (BaseWeb calendar) */
        [data-baseweb="calendar"] {
            background: #0f1730 !important;
            color: #e2e8f0 !important;
        }
        [data-baseweb="calendar"] button {
            color: #e2e8f0 !important;
        }
        [data-baseweb="calendar"] button[aria-selected="true"] {
            background: #38bdf8 !important;
            color: #020617 !important;
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
        _health_ck = _session_data_key()
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
                # Softer wording than "DEGRADED" — follows NWS advisory vocabulary.
                _status_label = "REDUCED CAPACITY"
                _status_color = "#b50909"  # USWDS error red
            elif _health_pct < 85:
                _status_label = "MONITORING"
                _status_color = "#ffbe2e"  # USWDS warning yellow
    except Exception:
        _status_label = "UNKNOWN"
        _status_color = "#71767a"  # USWDS gray

# Status banner — system status + live clock. Use st.html() to avoid
# markdown wrapping raw HTML in <p> tags that break flex layout.
_now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
# USWDS-style federal "official website" banner above the ops banner.
# Renders the standard GSA wording + small flag glyph so the service
# reads as a government tool from the first pixel.
_uswds_banner = (
    '<div class="usa-banner">'
    '<div class="usa-banner-inner">'
    '<span class="usa-banner-flag" aria-hidden="true">&#x1F1FA;&#x1F1F8;</span>'
    '<span class="usa-banner-text">'
    'An official NOAA / NWS / FAA operations tool. '
    'Data: NCEI, IEM, Aviation Weather Center, NWS, FAA WeatherCams, SWPC.'
    '</span>'
    '</div>'
    '</div>'
)
st.html(_uswds_banner)

_banner_html = (
    '<div class="ops-banner">'
    '<div class="ops-banner-left">'
    '<span class="banner-meta">ASOS NETWORK MONITORING &middot; OPERATIONAL</span>'
    '</div>'
    '<div class="ops-banner-right">'
    f'<span class="status-dot" style="background:{_status_color};'
    f'box-shadow:0 0 8px {_status_color};"></span>'
    f'<span class="status-label" style="color:{_status_color};">SYSTEM {_status_label}</span>'
    f'<span class="banner-meta banner-time">{_now_iso}</span>'
    '</div>'
    '</div>'
)
st.html(_banner_html)

# Title block.
st.html(
    '<div class="ops-title">'
    '<div class="ops-title-name">O.W.L.</div>'
    '<div class="ops-title-sub">Observation Watch Log &middot; National ASOS Operations Dashboard</div>'
    '</div>'
)

# Network health gauge.
if _health_pct is not None and _health_total:
    bar_color = ("#00a91c" if _health_pct >= 85
                 else ("#ffbe2e" if _health_pct >= 70 else "#b50909"))
    _gauge_html = (
        '<div class="health-card">'
        '<div class="health-row">'
        '<div class="health-meta">'
        f'<div class="health-label">NETWORK HEALTH &middot; LAST {_SCAN_HOURS}H</div>'
        '<div class="health-numbers">'
        f'<span class="health-pct">{_health_pct:.1f}%</span>'
        f'<span class="health-detail">{_health_clean:,} of {_health_total:,} stations reporting clean</span>'
        '</div>'
        '</div>'
        '<div class="health-stats">'
        f'<div class="health-stat"><span class="dot" style="background:#00a91c;"></span>{_health_clean:,} CLEAN</div>'
        f'<div class="health-stat"><span class="dot" style="background:#b50909;"></span>{_health_problem:,} ATTENTION</div>'
        '</div>'
        '</div>'
        '<div class="health-bar-track">'
        f'<div class="health-bar-fill" style="width:{_health_pct}%;background:{bar_color};"></div>'
        '</div>'
        '</div>'
    )
    st.html(_gauge_html)


# ===========================================================================
# Tabs
# ===========================================================================

#: Tab order deliberately puts the primary personas (AOMC controllers +
#: NWS forecasters) adjacent to the Summary landing page, with the
#: Reference / Reports / Admin tabs on the far right.  Title Case across
#: all tabs for a consistent federal-aesthetic.
tab_summary, tab_aomc, tab_fcst, tab_reports, tab_stations, tab_admin = st.tabs([
    "Summary", "AOMC Controllers", "NWS Forecasters",
    "Reports", "Stations", "Admin",
])


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

try:
    import pyarrow as _pa
    _ARROW_STR_DT = pd.ArrowDtype(_pa.string())
except Exception:  # pragma: no cover
    _ARROW_STR_DT = None


def _arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce columns into types Streamlit's Arrow JS frontend can deserialize.

    pandas 3.0 defaults to Arrow ``large_string`` (type 20) for all string
    columns, which Streamlit's ArrowTable.js reader rejects with
    ``Uncaught Error: Unrecognized type: "LargeUtf8" (20)``.

    Fix: convert every string-ish / list-valued column to
    ``pd.ArrowDtype(pa.string())``, which serializes to regular Arrow
    ``string`` (type 5) — supported by the frontend.

    Returns a copy; safe on empty / None.
    """
    if df is None or getattr(df, "empty", True):
        return df
    df = df.copy()
    for col in df.columns:
        dt = str(df[col].dtype).lower()
        # ---- timedelta -> total_seconds as float ------------------------
        if "timedelta" in dt:
            df[col] = df[col].dt.total_seconds().astype("float64")
            continue
        # ---- categorical -> string -------------------------------------
        if "category" in dt:
            df[col] = df[col].astype("object").astype(str)
            dt = str(df[col].dtype).lower()   # fallthrough to string path
        # ---- strings of any flavour ------------------------------------
        is_stringy = (
            dt == "object" or dt == "str" or "string" in dt
            or "large" in dt or "utf" in dt
        )
        if is_stringy:
            # Flatten list/tuple/set cells, then force a small-string
            # Arrow dtype so st.dataframe's Arrow transport works.
            def _coerce(v):
                if v is None:
                    return None
                if isinstance(v, float) and pd.isna(v):
                    return None
                if isinstance(v, (list, tuple, set)):
                    return ", ".join(str(x) for x in v)
                return str(v)
            values = [_coerce(v) for v in df[col].tolist()]
            if _ARROW_STR_DT is not None:
                try:
                    df[col] = pd.array(values, dtype=_ARROW_STR_DT)
                    continue
                except Exception:
                    pass
            # Fallback: plain object dtype.
            df[col] = pd.Series(values, index=df.index, dtype="object")
    return df


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
    # Flatten any list-valued columns (missing_hours_utc) into strings.
    return _arrow_safe(df)


#: Per-status cell background/foreground for the Status column.
_STATUS_CELL_STYLES = {
    "MISSING":      {"bg": "#fee2e2", "fg": "#7f1d1d"},
    "NO DATA":      {"bg": "#fecaca", "fg": "#7f1d1d"},
    "FLAGGED":      {"bg": "#fef3c7", "fg": "#78350f"},
    "INTERMITTENT": {"bg": "#ffedd5", "fg": "#7c2d12"},
    "RECOVERED":    {"bg": "#dbeafe", "fg": "#1e40af"},
    "CLEAN":        {"bg": "#dcfce7", "fg": "#14532d"},
}


def _style_status(df: pd.DataFrame, status_col: str) -> "pd.io.formats.style.Styler":
    """Return a pandas Styler that colors the status column by enum value."""
    def _row_style(row):
        styles = [""] * len(row)
        if status_col in row.index:
            v = str(row[status_col]).upper().strip()
            s = _STATUS_CELL_STYLES.get(v)
            if s:
                idx = list(row.index).index(status_col)
                styles[idx] = (
                    f"background-color:{s['bg']};color:{s['fg']};"
                    "font-weight:700;letter-spacing:0.04em;font-size:11px;"
                    "text-align:center;"
                )
        return styles
    return df.style.apply(_row_style, axis=1)


def _grid(df: pd.DataFrame, *, height: int = 380, key: str = "grid",
          selection: bool = False, pinned: list[str] | None = None,
          status_col: str | None = "status",
          prefer_aggrid: bool = False):
    """Render a DataFrame — uses ``st.dataframe`` by default.

    Native Streamlit tables are robust, accessible, and don't get silently
    blanked by the deep iframe nesting on HF Spaces.  AgGrid is still
    available (pass ``prefer_aggrid=True``) for the Stations catalog where
    filter UX matters most.

    The Status column, if present, is styled via pandas Styler with the
    same color palette the globe uses.
    """
    if df is None or df.empty:
        st.caption("(no rows)")
        return None

    # Arrow-safety pass — required for pandas 3.x (large_string dtypes).
    df = _arrow_safe(df)

    # ----- Default path: st.dataframe (robust, native) ------------------
    if not prefer_aggrid or not _HAVE_AGGRID:
        if status_col and status_col in df.columns:
            try:
                styled = _style_status(df, status_col)
                st.dataframe(
                    styled,
                    use_container_width=True,
                    height=height,
                    hide_index=True,
                )
                return None
            except Exception:
                logger.exception("status styling failed; rendering plain df")
        st.dataframe(df, use_container_width=True, height=height, hide_index=True)
        return None

    # ----- Opt-in: AgGrid (for big filterable tables) --------------------
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filterable=True, sortable=True, resizable=True,
        floatingFilter=True, wrapText=False, autoHeight=False,
    )
    gb.configure_grid_options(
        domLayout="normal",
        rowHeight=30,
        headerHeight=36,
        floatingFiltersHeight=32,
        animateRows=True,
        suppressMenuHide=False,
    )
    if selection:
        gb.configure_selection(selection_mode="single", use_checkbox=False)
    for col in (pinned or []):
        if col in df.columns:
            gb.configure_column(col, pinned="left", width=95)

    if status_col and status_col in df.columns:
        js = """
function(params) {
  if (!params.value) return null;
  const v = String(params.value).toUpperCase();
  const map = {
    'MISSING':      {bg:'#fee2e2', fg:'#7f1d1d'},
    'NO DATA':      {bg:'#fecaca', fg:'#7f1d1d'},
    'FLAGGED':      {bg:'#fef3c7', fg:'#78350f'},
    'INTERMITTENT': {bg:'#ffedd5', fg:'#7c2d12'},
    'RECOVERED':    {bg:'#dbeafe', fg:'#1e40af'},
    'CLEAN':        {bg:'#dcfce7', fg:'#14532d'},
  };
  const s = map[v];
  if (!s) return null;
  return {'backgroundColor': s.bg, 'color': s.fg,
          'fontWeight': '700', 'letterSpacing': '0.04em',
          'fontSize': '11px', 'textAlign': 'center'};
}
"""
        gb.configure_column(status_col, cellStyle=JsCode(js))

    opts = gb.build()
    is_dark = bool(st.session_state.get("dark_mode"))
    theme = "balham" if is_dark else "alpine"
    try:
        result = AgGrid(
            df,
            gridOptions=opts,
            height=height,
            theme=theme,
            update_mode=GridUpdateMode.SELECTION_CHANGED if selection else GridUpdateMode.NO_UPDATE,
            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
            allow_unsafe_jscode=True,
            key=key,
        )
        return result
    except Exception as e:
        logger.exception("AgGrid render failed; falling back to st.dataframe")
        st.warning(
            f"Interactive grid unavailable ({type(e).__name__}); "
            "showing simple table instead."
        )
        st.dataframe(df, use_container_width=True, height=height, hide_index=True)
        return None


# ---------------------------------------------------------------------------
# Background refresh scheduler (Tier 3)
# ---------------------------------------------------------------------------

def _sched_refresh_job():
    """Pre-compute the network-wide watchlist scan and push to diskcache."""
    try:
        if not _HAVE_AOMC:
            return
        end = datetime.now(timezone.utc)
        ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
        wl = build_watchlist(
            [_AOMC_META.get(sid, {"id": sid}) for sid in ids],
            hours=float(_SCAN_HOURS), end=end,
        )
        ck = _round_3min(end)
        if _HAVE_PC:
            _pc_put(ck, wl, ttl_seconds=600)
    except Exception:
        logger.exception("Scheduled refresh failed")


#: Background scheduler disabled by default — O.W.L. is fetch-on-demand.
#: Set ``OWL_ENABLE_BG_REFRESH=1`` to re-enable the 3-min refresh job.
import os as _os
if (_HAVE_SCHED
        and _os.environ.get("OWL_ENABLE_BG_REFRESH") == "1"
        and "sched_started" not in st.session_state):
    _sched = get_scheduler()
    if _sched is not None:
        schedule_watchlist_refresh(_sched, _sched_refresh_job, interval_minutes=3)
        st.session_state["sched_started"] = True


# ===========================================================================
# SUMMARY — public landing page. Interactive US map + KPIs + status tables.
# ===========================================================================

with tab_summary:
    _section_help(
        "About the Summary tab",
        what=(
            "The single-pane operational overview of the entire ASOS network. "
            "Shows a live 4-hour scan of all 920 federally-operated stations, "
            "rendered as a 3D globe with status-colored points, a news/alerts "
            "ticker, a click-to-drill station panel, a KPI row, and a sortable "
            "table of every station currently requiring attention."
        ),
        who=[
            "**NOC duty officers** — glance at the gauge + KPI row, spot the worst stations on the globe, drill into one in under 5 seconds.",
            "**AOMC controllers** — triage which stations need attention this shift before moving to the detailed Controllers tab.",
            "**NWS forecasters** — confirm a specific station is healthy before citing its METAR in a discussion.",
            "**Leadership / briefers** — export the summary KPIs for shift-change briefings.",
            "**First-time visitors** — the tab to open first to orient yourself.",
        ],
        how=[
            "Check the **Network Health** gauge for the overall % clean.",
            "Use the **REGION** buttons (CONUS / Northeast / West / Alaska / etc.) on the globe to zoom to an area of interest.",
            "Hover a point for a quick tooltip, or **click it** for a persistent card with the latest METAR + status reason.",
            "Use the **Drill into a station** selectbox below the globe to open the full station panel (METAR + webcams + CAP alerts).",
            "Open the **Status definitions** expander if any status label is unclear.",
            "Scroll to the **Stations Requiring Attention** table to see every non-clean station, worst-first.",
        ],
    )

    if not _HAVE_AOMC:
        st.error("AOMC catalog not loaded. Restart the app or contact your administrator.")
    else:
        all_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
        ck = _session_data_key()

        # Prefer persistent-cache hit first (populated by scheduler); fall
        # back to the in-process cached scan. Wrap in try/except so an
        # upstream outage (IEM 503) doesn't blow up the whole page.
        wl = _pc_get(ck) if _HAVE_PC else None
        _scan_err = None
        needs_fresh = wl is None or (hasattr(wl, "empty") and wl.empty)
        if needs_fresh:
            # Granular progress via st.status so cold-start users see
            # what's happening instead of a silent spinner for 60-90s.
            with st.status("Scanning network...", expanded=False) as scan_status:
                try:
                    scan_status.update(
                        label=f"Fetching METARs for {len(all_ids)} ASOS stations from IEM...",
                        state="running",
                    )
                    wl = _scan(all_ids, float(_SCAN_HOURS), ck)
                    if _HAVE_PC and wl is not None and not wl.empty:
                        scan_status.update(label="Caching results...", state="running")
                        _pc_put(ck, wl, ttl_seconds=600)
                    scan_status.update(
                        label=(
                            f"Scan complete: {len(wl) if wl is not None else 0} stations"
                            if wl is not None and not wl.empty
                            else "Scan finished (no data returned)"
                        ),
                        state="complete",
                    )
                except Exception as exc:
                    _scan_err = exc
                    scan_status.update(
                        label=f"Scan failed: {type(exc).__name__}",
                        state="error",
                    )
                    logger.exception("Summary scan failed")
                    wl = None

        if wl is None or wl.empty:
            if _scan_err is not None:
                st.error(
                    f"**Network scan temporarily unavailable.** Upstream "
                    f"data feed (IEM) returned an error: `{type(_scan_err).__name__}`. "
                    "The app will retry automatically on the next refresh."
                )
                st.caption("Other tabs (Reports, Forecasters, Stations) may still work "
                           "using independent data sources.")
            else:
                st.warning("No data returned. Check network connectivity.")
        else:
            cts = wl["status"].value_counts()
            nf = int(cts.get("FLAGGED", 0))
            ni = int(cts.get("INTERMITTENT", 0))
            nr = int(cts.get("RECOVERED", 0))
            nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
            nc = int(cts.get("CLEAN", 0))

            # ---- 3D satellite globe (Release #2 centerpiece) ----
            # Replaces the prior Folium 2D map.  WebGL globe via Globe.gl,
            # NASA Blue Marble texture, atmosphere glow, auto-rotate,
            # click-to-drill (postMessage to parent).  Falls back to the
            # 2D Folium map only if globe_view import failed.
            # --- Worst 5 Right Now strip ------------------------------
            # Five fixed-width cards above the globe surfacing the
            # station IDs a duty officer should look at first.  Ordered
            # by severity + minutes-since-last-report so MISSING /
            # long-silent stations float to the top.
            try:
                _sev_order = ["MISSING", "NO DATA", "FLAGGED",
                              "INTERMITTENT"]
                _attn = wl[wl["status"].astype(str).str.upper().str.strip()
                           .isin(_sev_order)].copy()
                if not _attn.empty and "minutes_since_last_report" in _attn:
                    _mins = pd.to_numeric(
                        _attn["minutes_since_last_report"],
                        errors="coerce").fillna(1e9)
                    _cat = pd.Categorical(
                        _attn["status"].astype(str).str.upper().str.strip(),
                        categories=_sev_order, ordered=True)
                    _attn = _attn.assign(_sev=_cat, _mins=_mins)
                    _attn = _attn.sort_values(
                        ["_sev", "_mins"], ascending=[True, False])
                    _worst = _attn.head(5)
                else:
                    _worst = _attn.head(5)

                if not _worst.empty:
                    st.markdown("**Worst 5 Right Now**")
                    w_cols = st.columns(min(5, len(_worst)))
                    _badge = {
                        "MISSING":  "#dc2626", "NO DATA":      "#fecaca",
                        "FLAGGED":  "#f59e0b", "INTERMITTENT": "#eab308",
                    }
                    for i, (_, row) in enumerate(_worst.iterrows()):
                        st_u = str(row.get("status", "")).upper().strip()
                        bc = _badge.get(st_u, "#64748b")
                        sid_s = _html_escape(row.get("station", ""))
                        name_s = _html_escape(
                            str(row.get("name", ""))[:22].title())
                        state_s = _html_escape(row.get("state", ""))
                        mins_v = row.get("minutes_since_last_report")
                        try:
                            mins_s = (f"{int(float(mins_v))} min ago"
                                      if mins_v is not None
                                      and pd.notna(mins_v) else "")
                        except Exception:
                            mins_s = ""
                        with w_cols[i]:
                            st.markdown(
                                f"<div style='padding:10px 12px;"
                                f"background:rgba(0,0,0,0.03);"
                                f"border-left:4px solid {bc};"
                                f"border-radius:4px;"
                                f"font-family:Inter,sans-serif;'>"
                                f"<div style='font-family:JetBrains Mono,"
                                f"monospace;font-size:16px;font-weight:700;"
                                f"color:#0f172a;'>{sid_s}</div>"
                                f"<div style='font-size:11px;color:#334155;'>"
                                f"{name_s} &middot; {state_s}</div>"
                                f"<div style='font-size:10px;color:{bc};"
                                f"font-weight:700;letter-spacing:0.08em;"
                                f"text-transform:uppercase;margin-top:4px;'>"
                                f"{_html_escape(st_u)}</div>"
                                f"<div style='font-size:10px;color:#64748b;"
                                f"margin-top:2px;'>{_html_escape(mins_s)}"
                                f"</div></div>",
                                unsafe_allow_html=True,
                            )
                    st.write("")
            except Exception:
                logger.exception("worst-5 strip failed")

            if _HAVE_GLOBE:
                st.subheader("National ASOS Status Globe")
                dark_mode = bool(st.session_state.get("dark_mode", True))

                # Pull news headlines for the bottom ticker. Feed failures
                # are swallowed (the aggregator returns [] on partial
                # outages, so a missing NOAA RSS never blanks the globe).
                news = []
                if _HAVE_NEWS:
                    try:
                        news = _cached_news(ck)
                    except Exception:
                        logger.exception("news fetch failed")

                # --- Optional radar + satellite overlay URLs ----------
                radar_url = None
                sat_url = None
                try:
                    from asos_tools.radar import (
                        latest_conus_radar_url,
                        latest_goes_conus_url,
                    )
                    radar_url = latest_conus_radar_url()
                    sat_url = latest_goes_conus_url("GEOCOLOR")
                except Exception:
                    logger.exception("radar/sat URL build failed")

                globe_html = build_globe_html(
                    wl,
                    station_meta=AOMC_STATIONS,
                    height_px=620,
                    # auto_rotate=False by default — federal operators
                    # find on-load rotation distracting + it burns GPU
                    # on always-on NOC displays.  The button remains.
                    auto_rotate=False,
                    dark=dark_mode,
                    show_atmosphere=True,
                    starfield=True,
                    news_items=news,
                    radar_overlay_url=radar_url,
                    satellite_overlay_url=sat_url,
                )
                st.components.v1.html(globe_html, height=660, scrolling=False)
                st.caption(
                    "Drag to rotate · scroll to zoom · click a point to "
                    "focus a station · hover ticker to pause. Auto-rotation "
                    "stops on interaction."
                )
                _section_help(
                    "About the RADAR and SATELLITE overlays",
                    what=(
                        "Two toggle buttons in the globe's top-right "
                        "control cluster layer federal-authoritative "
                        "imagery over the ASOS station points. **RADAR** "
                        "uses Iowa Environmental Mesonet's n0q CONUS "
                        "composite (transparent PNG, 5-minute cadence, "
                        "directly from the NWS NEXRAD feed). **SATELLITE** "
                        "uses NOAA NESDIS's GOES-19 GeoColor CONUS "
                        "latest image (5-minute cadence, day/night blended)."
                    ),
                    who=[
                        "**NWS forecasters** — overlay radar to correlate station thunderstorm flags against actual echoes.",
                        "**ATC supervisors** — at-a-glance situational awareness for which ARTCC sectors are taking weather.",
                        "**AOMC controllers** — confirm a `$`-flagged lightning sensor is actually in a storm, not a false positive.",
                        "**NOC duty officers** — keep the satellite layer on ambient for day/night cloud context.",
                    ],
                    how=[
                        "Click **RADAR** to toggle the n0q composite; click **SATELLITE** to toggle GOES-19 GeoColor. They can be layered together.",
                        "Layers are CONUS-only (bounds -126°E to -66°E, 24°N to 50°N). For Alaska or Hawaii, use the regional preset first — the overlay will still show if those stations are visible but the image covers lower-48 only.",
                        "Click again to toggle off. State persists until you reload the Space.",
                    ],
                    output=(
                        "Both layers refresh when the whole page refreshes "
                        "(every cold-load pulls the most recent tile). "
                        "Radar is transparent-PNG over the points; "
                        "satellite is opaque JPEG at reduced opacity so "
                        "the station dots remain visible on top."
                    ),
                )

                # ---- Station drill panel (below the globe) --------------
                # A selectbox drives the panel on the server side — sturdier
                # than an iframe->Streamlit postMessage bridge, and Ag/UX-
                # wise it doubles as a search-by-keystroke control.
                st.markdown("##### Drill into a station")
                plk = {s["id"]: s for s in AOMC_STATIONS if s.get("id")}
                options = sorted(plk.keys())
                # Default to the most severe flagged station if available.
                # Severity order: MISSING > NO DATA > FLAGGED > INTERMITTENT
                # > RECOVERED > CLEAN.  A plain `sort_values("status")` sorts
                # alphabetically (CLEAN -> FLAGGED -> ... -> RECOVERED), so we
                # need a categorical sort to open on the actual worst station.
                default_idx = 0
                try:
                    _severity_order = [
                        "MISSING", "NO DATA", "FLAGGED",
                        "INTERMITTENT", "RECOVERED", "CLEAN",
                    ]
                    cat = pd.Categorical(
                        wl["status"].astype(str).str.upper().str.strip(),
                        categories=_severity_order, ordered=True,
                    )
                    wl_sorted = wl.assign(_sev=cat).sort_values("_sev")
                    flagged_first = wl_sorted.iloc[0]
                    sid_flagged = str(flagged_first.get("station", ""))
                    if sid_flagged in options:
                        default_idx = options.index(sid_flagged)
                except Exception:
                    pass
                drill_sid = st.selectbox(
                    "Station",
                    options,
                    index=default_idx,
                    key="globe_drill_sid",
                    format_func=lambda s: (
                        f"{s} · "
                        f"{(plk.get(s, {}).get('name') or '').title()}"
                        f" · {plk.get(s, {}).get('state') or ''}"
                    ),
                )
                if drill_sid:
                    _render_drill_panel(drill_sid, plk, wl)
            elif _HAVE_FOLIUM:
                # Fallback: 2D Folium map if Globe.gl module isn't available.
                st.subheader("National ASOS Status Map (2D fallback)")
                with st.spinner("Rendering map…"):
                    fmap = build_status_map(
                        wl, AOMC_STATIONS,
                        cluster=True,
                        dark=bool(st.session_state.get("dark_mode")),
                        height_px=480,
                    )
                click = st_folium(
                    fmap,
                    height=480,
                    use_container_width=True,
                    returned_objects=["last_object_clicked_tooltip"],
                    key="summary_map",
                )
                if click and click.get("last_object_clicked_tooltip"):
                    clicked = str(click["last_object_clicked_tooltip"]).split(" ")[0]
                    if clicked:
                        st.caption(
                            f"Selected: `{clicked}` — open the **Reports** tab "
                            f"and enter this station ID for the full 1-min dashboard."
                        )
            else:
                st.caption(
                    "No interactive map available — install streamlit-folium "
                    "or ensure asos_tools.globe_view is importable."
                )

            st.divider()

            # ---- KPI metric row ----
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Clean", nc)
            c2.metric("Flagged (maintenance)", nf)
            c3.metric("Missing", nm)
            c4.metric("Recovered", nr)
            c5.metric("Intermittent", ni)
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

            # ---- Status definitions (collapsed, above the attention table) ----
            _render_status_glossary(key_suffix="summary", expanded=False)

            # ---- Status tables (use AgGrid where available) ----
            attention = _fmt_wl(wl, ["MISSING", "NO DATA", "FLAGGED", "INTERMITTENT", "RECOVERED"])
            if not attention.empty:
                st.subheader(f"Stations Requiring Attention ({len(attention)})")
                show = attention[[
                    "station", "name", "state", "status", "probable_reason",
                    "flagged", "total", "flag_rate",
                    "latest_time", "latest_flag_time",
                ]].rename(columns={
                    "station": "Station", "name": "Name", "state": "ST",
                    "status": "Status", "probable_reason": "Reason",
                    "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                    "latest_time": "Latest", "latest_flag_time": "Last $",
                })
                _grid(show, height=420, key="sum_attn",
                      pinned=["Station"], status_col="Status")

            st.caption(
                f"**{nc} stations** reporting clean — not listed. "
                "Use the **AOMC Controllers** tab for detailed per-category views."
            )


# ===========================================================================
# AOMC CONTROLLERS — auth-gated; 3 sub-tabs: METARs / Missing METARs / Flags
# ===========================================================================

with tab_aomc:
    st.markdown("### AOMC Controller Workstation")
    st.caption(
        "ASOS Operations and Monitoring Center view — intended for "
        "controllers responsible for the 920 federally-operated stations."
    )
    _section_help(
        "About the AOMC Controllers tab",
        what=(
            "A per-station deep-dive intended for technicians whose job is "
            "keeping the 920-station ASOS fleet healthy. Three sub-tabs "
            "(METARs, Missing METARs, Maintenance Flags) partition the "
            "fleet so you can focus on one operational concern at a time. "
            "Each sub-tab shows a sortable, filterable, CSV-exportable table "
            "plus a 'Build PDF Briefing' button for shift handoffs."
        ),
        who=[
            "**AOMC controllers** (primary audience) — the whole tab exists for you.",
            "**Field maintenance techs** — pull the Maintenance Flags table, sort by latest flag time, drive to the nearest flagged station.",
            "**Ops managers** — export the PDF briefing at shift-change so the oncoming controller has exactly the info the offgoing one was working from.",
            "**QA / compliance** — archive the CSV exports to prove the fleet was monitored.",
        ],
        how=[
            "Pick a **Scan window** (1h for 'right now', 24h for an end-of-shift report).",
            "Open the sub-tab matching your concern: **METARs** for the full live feed, **Missing METARs** for silent stations, **Maintenance Flags** for the `$` stations.",
            "Use the table's column headers to sort/filter. Right-click a row to copy/export; click **Download CSV** for the whole view.",
            "Click **Build PDF Briefing** to generate a shift-handoff document including every flagged/missing station with decoded reasons.",
            "Open the **Status definitions** expander below if any label is unclear.",
        ],
    )
    _render_status_glossary(key_suffix="aomc", expanded=False)

    if _HAVE_AOMC:
        # Use a wider default scan for controllers.
        aomc_hours = st.selectbox(
            "Scan window",
            [1, 2, 4, 6, 12, 24],
            index=2,
            format_func=lambda h: f"Last {h} hours",
            key="aomc_hours",
        )
        aomc_ck = _session_data_key()
        aomc_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
        wl_aomc = None
        _aomc_err = None
        with st.spinner(f"Scanning {len(aomc_ids)} stations over {aomc_hours}h…"):
            try:
                wl_aomc = _scan(aomc_ids, float(aomc_hours), aomc_ck)
            except Exception as exc:
                _aomc_err = exc
                logger.exception("AOMC scan failed")

        if wl_aomc is None or wl_aomc.empty:
            if _aomc_err is not None:
                st.error(
                    f"**Controller scan temporarily unavailable.** Upstream "
                    f"IEM METAR feed returned `{type(_aomc_err).__name__}`. "
                    "Retry in a moment — IEM is intermittent under load."
                )
            else:
                st.warning("No data returned.")
        else:
            cts = wl_aomc["status"].value_counts()
            nf = int(cts.get("FLAGGED", 0))
            nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
            nr = int(cts.get("RECOVERED", 0))
            ni = int(cts.get("INTERMITTENT", 0))
            nc = int(cts.get("CLEAN", 0))
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Clean", nc)
            c2.metric("Flagged (maintenance)", nf)
            c3.metric("Missing", nm)
            c4.metric("Recovered", nr)
            c5.metric("Intermittent", ni)

            # --- Per-operator rollup -----------------------------------
            # NWS / FAA / DOD / Navy each answer to a different boss.
            # Derived from station_types ("ASOS", "ASOS,MILITARY",
            # "ASOS,COOP", etc.) joined from the AOMC catalog.
            try:
                _sid_to_types = {
                    s["id"]: (s.get("station_types") or "")
                    for s in AOMC_STATIONS if s.get("id")
                }
                def _operator_of(row) -> str:
                    types = _sid_to_types.get(row.get("station", ""), "")
                    if "NAVY" in types:   return "Navy"
                    if "MILITARY" in types: return "DOD"
                    if "FAA" in types:    return "FAA"
                    return "NWS"
                _op_s = wl_aomc.assign(_op=wl_aomc.apply(_operator_of, axis=1))
                _op_counts = (
                    _op_s.groupby("_op")["status"]
                         .value_counts().unstack(fill_value=0)
                         .reindex(columns=["CLEAN", "FLAGGED", "MISSING",
                                           "INTERMITTENT", "RECOVERED",
                                           "NO DATA"], fill_value=0)
                         .reset_index().rename(columns={"_op": "Operator"})
                )
                st.markdown("**Per-operator rollup**")
                st.dataframe(_op_counts, hide_index=True,
                             use_container_width=True)
            except Exception:
                logger.exception("per-operator rollup failed")

            sub_metars, sub_missing, sub_flags = st.tabs([
                "METARs", "Missing METARs", "Maintenance Flags",
            ])

            # -- Sub-tab A: METARs (latest METAR per station) ---------------
            with sub_metars:
                st.markdown(
                    "**Latest METAR for every reporting station.** "
                    "Filter, sort, and pin columns as needed; export via "
                    "the right-click menu."
                )
                metars_df = wl_aomc[[
                    "station", "name", "state", "status", "probable_reason",
                    "flagged", "total", "flag_rate", "minutes_since_last_report",
                    "latest_metar",
                ]].copy().rename(columns={
                    "station": "Station", "name": "Name", "state": "ST",
                    "status": "Status", "probable_reason": "Reason",
                    "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                    "minutes_since_last_report": "Min since report",
                    "latest_metar": "Latest METAR",
                })
                # Cast object to string for clean display
                metars_df["Latest METAR"] = metars_df["Latest METAR"].fillna("").astype(str)
                _grid(metars_df, height=520, key="aomc_metars",
                      pinned=["Station"], status_col="Status")
                st.download_button(
                    "Download CSV",
                    metars_df.to_csv(index=False).encode(),
                    f"aomc_metars_{aomc_hours}h.csv",
                    "text/csv",
                )

            # -- Sub-tab B: Missing METARs ----------------------------------
            with sub_missing:
                st.markdown(
                    "**Stations missing scheduled hourly METARs.** "
                    "ASOS routine: one report per hour near HH:51Z. "
                    "A silent station is more critical than a flagged one."
                )
                view = _fmt_wl(wl_aomc, ["MISSING", "NO DATA"])
                if view.empty:
                    st.success("No stations are currently missing scheduled METARs.")
                else:
                    show = view[[
                        "station", "name", "state", "status",
                        "missing", "expected_hourly", "missing_hours_utc",
                        "minutes_since_last_report",
                    ]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "status": "Status",
                        "missing": "Gaps", "expected_hourly": "Expected",
                        "missing_hours_utc": "Missing Hours",
                        "minutes_since_last_report": "Min since report",
                    })
                    _grid(show, height=500, key="aomc_missing",
                          pinned=["Station"], status_col="Status")
                    st.download_button(
                        "Download CSV",
                        show.to_csv(index=False).encode(),
                        f"aomc_missing_{aomc_hours}h.csv",
                        "text/csv",
                    )
                    if _HAVE_APPRISE:
                        if st.button("Notify alert recipients",
                                     key="aomc_missing_notify",
                                     help="Send an Apprise alert for each missing station."):
                            sent_total, failed_total = 0, 0
                            for _, row in view.iterrows():
                                s, f = owl_alerts.send_missing_alert(row)
                                sent_total += s
                                failed_total += f
                            if sent_total:
                                st.success(f"Sent {sent_total} missing-station alerts.")
                            elif failed_total:
                                st.error(f"All {failed_total} alert deliveries failed.")
                            else:
                                st.info("No Apprise URLs configured — set `OWL_ALERT_URLS` env var.")
                with st.expander("Why do METARs go missing?"):
                    st.markdown("""
Common causes: power outage, communication failure, station
decommissioned/seasonal, sensor cascade failure, or IEM ingestion lag.
                    """)

            # -- Sub-tab C: Maintenance Flags ($) ---------------------------
            with sub_flags:
                st.markdown(
                    "**Stations with the `$` maintenance-check indicator.** "
                    "The `$` flag signals an out-of-tolerance condition — "
                    "it does not necessarily mean data is inaccurate."
                )
                show_clean = st.checkbox("Include clean", key="aomc_flags_c")
                keep = ["FLAGGED", "INTERMITTENT", "RECOVERED"]
                if show_clean:
                    keep.append("CLEAN")
                view = _fmt_wl(wl_aomc, keep)
                if view.empty:
                    st.success("No stations are currently flagged.")
                else:
                    show = view[[
                        "station", "name", "state", "status",
                        "probable_reason", "flagged", "total", "flag_rate",
                        "latest_time", "latest_flag_time",
                    ]].rename(columns={
                        "station": "Station", "name": "Name", "state": "ST",
                        "status": "Status", "probable_reason": "Reason",
                        "flagged": "$", "total": "Total", "flag_rate": "Rate %",
                        "latest_time": "Latest", "latest_flag_time": "Last $",
                    })
                    _grid(show, height=500, key="aomc_flags",
                          pinned=["Station"], status_col="Status")
                    st.download_button(
                        "Download CSV",
                        show.to_csv(index=False).encode(),
                        f"aomc_flags_{aomc_hours}h.csv",
                        "text/csv",
                    )
                    if _HAVE_APPRISE:
                        if st.button("Notify alert recipients",
                                     key="aomc_flags_notify",
                                     help="Send an Apprise alert for each flagged station."):
                            sent_total, failed_total = 0, 0
                            for _, row in view[view["status"] == "FLAGGED"].iterrows():
                                s, f = owl_alerts.send_flag_alert(row)
                                sent_total += s
                                failed_total += f
                            if sent_total:
                                st.success(f"Sent {sent_total} flag alerts.")
                            elif failed_total:
                                st.error(f"All {failed_total} alert deliveries failed.")
                            else:
                                st.info("No Apprise URLs configured — set `OWL_ALERT_URLS` env var.")

                with st.expander("What does `$` mean?"):
                    st.markdown("""
The `$` is the ASOS maintenance-check indicator. Sensor-specific codes
decoded from METAR remarks: **RVRNO** (RVR), **PWINO** (precip ID),
**PNO** (precip gauge), **FZRANO** (freezing rain), **TSNO** (lightning),
**VISNO** (visibility), **CHINO** (ceilometer). Most flags show
"Internal check" — tolerance drift with no specific sensor code
in the remarks.
                    """)

            # PDF export for the whole AOMC watchlist
            if _HAVE_PDF:
                st.divider()
                st.markdown("**Export shift summary**")
                ccol1, ccol2 = st.columns([1, 2])
                with ccol1:
                    if st.button("Build PDF briefing", key="aomc_pdf_btn",
                                 type="primary"):
                        st.session_state["aomc_pdf"] = build_watchlist_pdf(
                            wl_aomc, title="O.W.L. AOMC Shift Briefing",
                            window_hours=aomc_hours, group_label="All AOMC",
                        )
                with ccol2:
                    if st.session_state.get("aomc_pdf"):
                        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%MZ")
                        st.download_button(
                            "Download PDF",
                            data=st.session_state["aomc_pdf"],
                            file_name=f"owl_aomc_briefing_{ts}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )


# ===========================================================================
# REPORTS — station-level reports (1-min dashboard, $ analysis, incident)
# ===========================================================================

with tab_reports:
    st.markdown("### Station Reports")
    st.caption(
        "Generate 1-minute dashboards, maintenance analyses, and formal "
        "DOCX/PDF incident reports."
    )
    _section_help(
        "About the Reports tab",
        what=(
            "On-demand reports for a single station or a group of stations "
            "over any date range. Produces three artifact types: (a) a "
            "matplotlib **dashboard PNG** with wind rose + time series + "
            "annotated events; (b) a **Missing METAR** report for "
            "availability audits; (c) a **formal DOCX/PDF incident report** "
            "with an executive summary, per-station timelines, `$` evidence, "
            "sensor-code breakdowns, and recommendations."
        ),
        who=[
            "**AOMC controllers** — generate the daily dashboard PNG for stations that tripped `$` overnight.",
            "**Incident investigators** — produce the formal DOCX when a station had a maintenance event that requires a paper trail.",
            "**Shift supervisors** — pull a Missing METAR report to document availability compliance.",
            "**Program managers** — ZIP a 7-day dashboard bundle for a whole region for a monthly review.",
            "**Researchers** — export the underlying 1-minute CSV for further analysis.",
        ],
        how=[
            "Pick a **station or group** (Long Island, Front Range, etc.) and a **date range**.",
            "Choose the **report type**: Dashboard / Maintenance / Missing / Incident.",
            "Click **Generate** — the report appears inline with a download button.",
            "For multi-station jobs use the **Bulk ZIP** option to bundle every report into one archive.",
            "Incident reports accept a free-text 'Controller observations' field that feeds directly into the DOCX narrative.",
        ],
        output=(
            "Dashboards are PNG (1500×1000 px, publication-quality). "
            "Incident reports are DOCX/PDF with NOAA-blue banners and "
            "FOUO-style headings. All filenames encode station + date range, "
            "so a shift's worth of reports sorts chronologically."
        ),
    )

    # --- Inline report configuration (moved from sidebar) ----------------
    with st.container():
        rc1, rc2 = st.columns([1, 1])
        with rc1:
            report_type = st.selectbox(
                "Report type",
                ["1-min dashboard", "Maintenance ($)", "Flagged vs clean",
                 "Missing METARs", "Incident report (DOCX)"],
                key="rep_type",
            )
            smode = st.radio("Station source", ["Single", "Group", "Custom"],
                             horizontal=True, key="rep_smode")
        with rc2:
            wmode = st.selectbox(
                "Time window",
                ["1 day", "7 days", "14 days", "30 days", "Custom"],
                index=1, key="rep_wmode",
            )
            if wmode == "Custom":
                today = datetime.now(timezone.utc).date()
                dc1, dc2 = st.columns(2)
                with dc1:
                    sd = st.date_input("From", today - timedelta(days=7),
                                       key="rep_sd")
                with dc2:
                    ed = st.date_input("To", today, key="rep_ed")
                start = datetime.combine(sd, datetime.min.time(), tzinfo=timezone.utc)
                end = datetime.combine(ed, datetime.min.time(), tzinfo=timezone.utc)
                span = (end - start).days
                if span > _MAX_CUSTOM_DAYS:
                    st.error(f"Date range too large ({span} days). Max {_MAX_CUSTOM_DAYS}.")
                elif span <= 0:
                    st.error("End date must be after start date.")
                wlabel = _wlabel(max(span, 1))
            else:
                days = {"1 day": 1, "7 days": 7, "14 days": 14, "30 days": 30}[wmode]
                end = _round_5min(datetime.now(timezone.utc))
                start = _round_5min(end - timedelta(days=days))
                wlabel = _wlabel(days)

        stations_list: list[str] = []
        group_label = ""

        if smode == "Single":
            aomc_toggle = st.toggle("AOMC stations only", value=True, key="rep_aomc_tog") if _HAVE_AOMC else False
            if _HAVE_CATALOG:
                pool = ([s for s in ALL_ASOS_STATIONS if s["id"] in AOMC_IDS]
                        if aomc_toggle else ALL_ASOS_STATIONS)
                pool_ids = [s["id"] for s in pool]
                _plk = {s["id"]: s for s in pool}
                sid = st.selectbox(
                    "Station", pool_ids,
                    index=pool_ids.index("KJFK") if "KJFK" in pool_ids else 0,
                    format_func=lambda s: (
                        f"{s} · {_short_name(_plk.get(s, {}).get('name', ''))} · "
                        f"{_plk.get(s, {}).get('state', '')}"
                    ),
                    key="rep_sid",
                )
            else:
                sid = st.text_input("ICAO ID", "KJFK", key="rep_sid_txt").strip().upper()
            stations_list = [sid]
            group_label = sid
        elif smode == "Group":
            grp = st.selectbox("Preset group", list_groups(),
                               format_func=lambda s: s.replace("_", " ").title(),
                               key="rep_grp")
            stations_list = list(get_group(grp))
            group_label = grp.replace("_", " ").title()
            st.caption(f"{len(stations_list)} stations")
        else:
            raw = st.text_area("Station IDs (comma or newline)", "KJFK, KLGA, KEWR",
                               height=68, key="rep_raw")
            stations_list = [s.strip().upper() for s in
                             raw.replace(",", "\n").splitlines() if s.strip()]
            group_label = ", ".join(stations_list[:3]) + ("…" if len(stations_list) > 3 else "")
            st.caption(f"{len(stations_list)} stations")

        go = st.button("Generate report", type="primary",
                       use_container_width=True, key="rep_go")

    st.divider()

    if not go:
        st.info("Pick a report type, station(s), and window, then click **Generate report**.")
    else:
        kpi_slot = st.empty()
        report_slot = st.empty()
        download_slot = st.empty()
        preview_slot = st.empty()

        try:
            if report_type == "Incident report (DOCX)":
                if not stations_list:
                    report_slot.error("Select at least one station.")
                else:
                    span_hours = max(1.0, (end - start).total_seconds() / 3600)
                    with kpi_slot.container():
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Stations", len(stations_list))
                        c2.metric("Window", wlabel)
                        c3.metric("Investigation depth", f"{span_hours:.0f}h")
                    with st.spinner(f"Investigating {len(stations_list)} station(s)…"):
                        docx_bytes = generate_incident_docx(
                            stations_list, hours=span_hours, end=end)
                    with report_slot.container():
                        st.success(
                            f"Incident investigation complete. "
                            f"Report: {len(docx_bytes):,} bytes · {len(stations_list)} station(s)."
                        )
                        st.info(
                            "The DOCX report includes: executive summary · per-station "
                            "incident timelines · raw $ METAR evidence · sensor code "
                            "breakdown · root-cause analysis · recommendations."
                        )
                    ts = end.strftime("%Y%m%d_%H%MZ")
                    slug = "_".join(s.lower() for s in stations_list[:4])
                    if len(stations_list) > 4:
                        slug += f"_plus{len(stations_list) - 4}"
                    with download_slot.container():
                        st.download_button(
                            "Download incident report (DOCX)",
                            data=docx_bytes,
                            file_name=f"Incident_Report_{slug}_{ts}.docx",
                            mime=("application/vnd.openxmlformats-officedocument."
                                  "wordprocessingml.document"),
                            use_container_width=True,
                        )

            elif report_type == "1-min dashboard":
                stn = stations_list[0]
                with st.spinner(f"Fetching {stn}…"):
                    df = _fetch_1min(stn, start, end)
                if df.empty:
                    report_slot.error("No data for this station/window.")
                else:
                    name = str(df["station_name"].iloc[0])
                    with kpi_slot.container():
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Station", stn)
                        c2.metric("Name", _short_name(name))
                        c3.metric("Window", wlabel)
                        c4.metric("Observations", f"{len(df):,}")
                    with st.spinner("Rendering…"):
                        png = _render(build_report, df=df, window_label=wlabel,
                                      station_id=stn, station_name=name)
                    report_slot.image(png, use_container_width=True)
                    with download_slot.container():
                        dc1, dc2, dc3 = st.columns(3)
                        dc1.download_button(
                            "Download PNG", png,
                            f"{stn}_{wlabel.replace(' ', '')}.png",
                            "image/png", use_container_width=True,
                        )
                        dc2.download_button(
                            "Download CSV",
                            df.to_csv(index=False).encode(),
                            f"{stn}_{wlabel.replace(' ', '')}.csv",
                            "text/csv", use_container_width=True,
                        )
                        if _HAVE_PDF:
                            pdf_bytes = build_report_pdf(
                                png,
                                title=f"{stn} — 1-Minute Dashboard",
                                subtitle=f"{_short_name(name)} · {wlabel} · {len(df):,} obs",
                                body_text=(
                                    f"1-minute ASOS observations for {stn} "
                                    f"({_short_name(name)}) over the "
                                    f"{wlabel} ending "
                                    f"{end.strftime('%Y-%m-%d %H:%MZ')}."
                                ),
                            )
                            dc3.download_button(
                                "Download PDF", pdf_bytes,
                                f"{stn}_{wlabel.replace(' ', '')}.pdf",
                                "application/pdf", use_container_width=True,
                            )
                    with preview_slot.container():
                        with st.expander("Data preview (50 rows)"):
                            st.dataframe(_arrow_safe(df.head(50)),
                                         use_container_width=True, height=300)

            else:
                with st.spinner("Fetching METARs…"):
                    metars = _fetch_metars(tuple(stations_list), start, end)
                if metars.empty:
                    report_slot.error("No METARs for this selection/window.")
                else:
                    with kpi_slot.container():
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
                    report_slot.image(png, use_container_width=True)
                    slug = group_label.lower().replace(" ", "-").replace(",", "")
                    with download_slot.container():
                        dc1, dc2, dc3 = st.columns(3)
                        dc1.download_button(
                            "Download PNG", png,
                            f"{slug}_{wlabel.replace(' ', '')}_{kind}.png",
                            "image/png", use_container_width=True,
                        )
                        dc2.download_button(
                            "Download CSV",
                            metars.to_csv(index=False).encode(),
                            f"{slug}_{wlabel.replace(' ', '')}_metars.csv",
                            "text/csv", use_container_width=True,
                        )
                        if _HAVE_PDF:
                            pdf_bytes = build_report_pdf(
                                png,
                                title=f"{report_type} — {group_label}",
                                subtitle=f"{wlabel} · {len(metars):,} METARs · "
                                         f"{metars['station'].nunique()} stations",
                                body_text=(
                                    f"METAR-derived report ({report_type}) for "
                                    f"{group_label} over the {wlabel} ending "
                                    f"{end.strftime('%Y-%m-%d %H:%MZ')}."
                                ),
                            )
                            dc3.download_button(
                                "Download PDF", pdf_bytes,
                                f"{slug}_{wlabel.replace(' ', '')}_{kind}.pdf",
                                "application/pdf", use_container_width=True,
                            )
                    with preview_slot.container():
                        with st.expander("METAR data preview (50 rows)"):
                            st.dataframe(_arrow_safe(metars.head(50)),
                                         use_container_width=True, height=300)

        except Exception as e:
            logger.exception("Report generation failed")
            st.error(f"Could not generate report: {e}. Check station ID and time window.")


# ===========================================================================
# STATIONS — AOMC directory (AgGrid)
# ===========================================================================

with tab_stations:
    st.subheader("AOMC Federal Station Directory")
    st.caption(
        f"{len(AOMC_STATIONS)} stations from NCEI HOMR — "
        "NWS / FAA / DOD operated ASOS sites."
    )
    _section_help(
        "About the Stations tab",
        what=(
            "The master directory of every federally-operated ASOS station "
            "in the network. Each row is pulled from NCEI HOMR (NOAA's "
            "authoritative Historical Observing Metadata Repository). "
            "Fields include ICAO call, NCDC/WBAN IDs, operator (NWS/FAA/DOD), "
            "lat/lon, elevation, begin-of-operation date, and station type."
        ),
        who=[
            "**Controllers looking up a station** — search by ICAO, name, or state to pull metadata.",
            "**Analysts comparing stations** — filter by state/operator to compose custom groups.",
            "**Researchers** — export the full directory as CSV for offline analysis.",
            "**Newcomers learning the network** — browse to get a sense of the station footprint.",
        ],
        how=[
            "Use the **search box** to filter by ICAO, name, state, or operator.",
            "Click any column header to **sort**.",
            "Check a row's checkbox (if selection is enabled) to preview its location on the map.",
            "Use **Download CSV** to export the currently-filtered view.",
        ],
        output=(
            "The table is rendered with the full AgGrid feature set (sort, "
            "filter, resize, pin) because a 920-row directory genuinely "
            "benefits from interactive filtering. Elsewhere in the app we "
            "use st.dataframe; here AgGrid is the right tool."
        ),
    )
    if _HAVE_AOMC:
        c1, c2 = st.columns([3, 1])
        with c1:
            q = st.text_input("Search", placeholder="ID, name, or county",
                              label_visibility="collapsed", key="stn_q")
        with c2:
            states = sorted({s.get("state") for s in AOMC_STATIONS if s.get("state")})
            sf = st.selectbox("State", ["All"] + states,
                              label_visibility="collapsed", key="stn_sf")

        rows = AOMC_STATIONS
        if sf != "All":
            rows = [s for s in rows if s.get("state") == sf]
        if q:
            qu = q.upper()
            rows = [s for s in rows
                    if qu in (s.get("id") or "").upper()
                    or qu in (s.get("name") or "").upper()
                    or qu in (s.get("county") or "").upper()]

        st.caption(f"{len(rows):,} stations")
        df = pd.DataFrame([{
            "ICAO": s.get("id"), "Call": s.get("call"),
            "Name": s.get("name"), "State": s.get("state"),
            "County": s.get("county"), "Lat": s.get("lat"),
            "Lon": s.get("lon"), "Elev (ft)": s.get("elev_ft"),
            "WBAN": s.get("wban"), "Types": s.get("station_types"),
        } for s in rows])
        _grid(df, height=580, key="stn_grid", pinned=["ICAO"],
              status_col=None)
        if not df.empty:
            st.download_button("Download CSV", df.to_csv(index=False).encode(),
                               "aomc_stations.csv", "text/csv")
    else:
        st.error("Catalog not loaded.")


# ===========================================================================
# NWS FORECASTERS — aviation-oriented regional summary
# ===========================================================================

with tab_fcst:
    st.markdown("### NWS Forecaster Workstation")
    st.caption(
        "Aviation weather ops console. Cross-sources live data from the "
        "Aviation Weather Center, NWS CAP alerts, and the ASOS network."
    )
    _section_help(
        "About the NWS Forecasters tab",
        what=(
            "A four-pane aviation weather ops console. Pane 1 shows national "
            "hazards (SIGMETs, AIRMETs, G-AIRMETs, TFRs). Pane 2 gives a "
            "side-by-side METAR + TAF view for any station. Pane 3 rolls up "
            "the entire AOMC fleet by flight category (VFR/MVFR/IFR/LIFR). "
            "Pane 4 shows active NWS CAP alerts nationwide, filterable by "
            "severity."
        ),
        who=[
            "**NWS aviation forecasters** — primary audience; build the morning hazard brief.",
            "**ATC facility supervisors** — scan National Hazards before coordinating around weather with AOMC.",
            "**Pilots / dispatchers preparing a flight** — pull the TAF + nearby PIREPs for a destination.",
            "**Emergency managers** — Active Alerts shows every NWS CAP warning in force right now.",
            "**Anyone wanting 'is KXYZ flyable right now'** — Station TAF/METAR gives the cleanest single-page answer.",
        ],
        how=[
            "Start at **National Hazards** to see what's being watched fleet-wide.",
            "Switch to **Station TAF/METAR**, enter or pick an ICAO, and read current + forecast conditions.",
            "Use **Flight Category Rollup** to see how many stations are in each category right now (red flags if IFR/LIFR > baseline).",
            "Check **Active Alerts** last — filter by severity (Extreme/Severe/Moderate/Minor) to focus on actionable threats.",
        ],
        output=(
            "Everything in this tab comes from the federal Aviation Weather "
            "Center (aviationweather.gov) and NWS api.weather.gov — no "
            "scraping, all authoritative primary sources, refreshed live."
        ),
    )
    fcst_ck = _session_data_key()

    fcst_a, fcst_b, fcst_c, fcst_d, fcst_e = st.tabs([
        "National Hazards", "Station TAF / METAR",
        "Flight Category Rollup", "Active Alerts",
        "Space Weather",
    ])

    # ---- A. National Hazards (SIGMET + AIRMET + PIREP) -----------------
    with fcst_a:
        st.markdown("**Active SIGMETs & AIRMETs** (AWC)")
        if _HAVE_AWC:
            with st.spinner("Fetching AWC airsigmets…"):
                sigs = _awc_airsigmet(fcst_ck)
            if sigs:
                rows = []
                for s in sigs:
                    rows.append({
                        "Type": s.get("airSigmetType") or s.get("hazard") or "?",
                        "Hazard": s.get("hazard") or "",
                        "Severity": s.get("severity") or "",
                        "Area": (s.get("area") or s.get("rawAirSigmet") or "")[:80],
                        "Valid From": s.get("validTimeFrom") or "",
                        "Valid To":   s.get("validTimeTo") or "",
                    })
                _grid(pd.DataFrame(rows), height=280, key="fc_sigs",
                      pinned=["Type"], status_col="Severity")
            else:
                st.success("No active SIGMETs or AIRMETs at this time.")

            st.divider()
            st.markdown("**Recent PIREPs** (last 2 hours, CONUS)")
            with st.spinner("Fetching pilot reports…"):
                pireps = _awc_pirep(fcst_ck)
            if pireps:
                pr_rows = []
                for p in pireps[:60]:
                    pr_rows.append({
                        "Station": p.get("icaoId") or "",
                        "Type":    p.get("pirepType") or "",
                        "FL":      p.get("fltLvl") or "",
                        "Aircraft":p.get("acType") or "",
                        "Turb":    p.get("tbInt1") or "",
                        "Icing":   p.get("icgInt1") or "",
                        "Report":  (p.get("rawOb") or "")[:200],
                    })
                _grid(pd.DataFrame(pr_rows), height=340, key="fc_pireps",
                      pinned=["Station"], status_col=None)
            else:
                st.caption("No PIREPs in the last 2 hours.")
        else:
            st.error("AWC client not installed.")

    # ---- B. Station TAF / METAR lookup ---------------------------------
    with fcst_b:
        st.markdown("**Station lookup** — live METAR + TAF from AWC")
        fc_sid = st.text_input(
            "ICAO ID", "KJFK", key="fcst_sid",
            help="Enter any ICAO; works globally for AWC-covered stations.",
        ).strip().upper()
        if fc_sid and re.fullmatch(r"[A-Z0-9]{3,6}", fc_sid):
            if _HAVE_AWC:
                with st.spinner(f"Fetching {fc_sid}…"):
                    metars = _awc_metar_bulk((fc_sid,), fcst_ck)
                    tafs = _awc_taf(fc_sid, fcst_ck)
                # Current METAR summary
                if metars:
                    m = metars[0]
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Temp",
                              f"{m.get('temp', '—')}°C" if m.get('temp') is not None else "—")
                    c2.metric("Dew",
                              f"{m.get('dewp', '—')}°C" if m.get('dewp') is not None else "—")
                    c3.metric("Wind",
                              f"{m.get('wdir','—')}° / {m.get('wspd','—')} kt" if m.get('wspd') is not None else "—")
                    c4.metric("Vis",
                              f"{m.get('visib','—')} SM" if m.get('visib') is not None else "—")
                    fc = owl_awc.flight_category(
                        float(m.get('visib')) if isinstance(m.get('visib'), (int, float)) else None,
                        float(m.get('ceil')) if isinstance(m.get('ceil'), (int, float)) else None,
                    )
                    fc_color = {"VFR": "#00a91c", "MVFR": "#38bdf8",
                                "IFR": "#ffbe2e", "LIFR": "#b50909"}.get(fc, "#64748b")
                    st.markdown(
                        f'<div style="margin:8px 0;padding:6px 12px;background:{fc_color};'
                        f'color:#fff;font-weight:700;border-radius:6px;display:inline-block;">'
                        f'Flight category: {fc}</div>',
                        unsafe_allow_html=True,
                    )
                    st.code((m.get("rawOb") or "(no METAR text)"), language="text")
                else:
                    st.warning(f"No AWC METAR for {fc_sid}.")
                # TAF
                if tafs:
                    t = tafs[0]
                    st.markdown("**TAF** (Terminal Aerodrome Forecast)")
                    raw_taf = t.get("rawTAF") or t.get("rawOb") or ""
                    st.code(raw_taf or "(no TAF)", language="text")
                else:
                    st.caption(f"No TAF available for {fc_sid}.")
            else:
                st.error("AWC client not installed.")
        elif fc_sid:
            st.error("Invalid ICAO (must be 3–6 letters/digits).")

    # ---- C. Flight Category Rollup (derive from watchlist) -------------
    with fcst_c:
        st.markdown("**AOMC Network flight-category & state rollup**")
        if _HAVE_AOMC:
            all_ids = tuple(s["id"] for s in AOMC_STATIONS if s.get("id"))
            wl_fc = _pc_get(fcst_ck) if _HAVE_PC else None
            _fc_err = None
            if wl_fc is None or (hasattr(wl_fc, "empty") and wl_fc.empty):
                with st.spinner("Scanning network…"):
                    try:
                        wl_fc = _scan(all_ids, float(_SCAN_HOURS), fcst_ck)
                    except Exception as exc:
                        _fc_err = exc
                        logger.exception("Forecaster flight-category scan failed")
                        wl_fc = None
            if wl_fc is None or wl_fc.empty:
                if _fc_err is not None:
                    st.error(
                        f"Rollup unavailable: {type(_fc_err).__name__}. "
                        "Try the National Hazards or Station TAF sub-tabs "
                        "— those use AWC, an independent source."
                    )
                else:
                    st.warning("No data available.")
            else:
                by_state = (
                    wl_fc.assign(bad=wl_fc["status"].isin(
                        ["MISSING", "NO DATA", "FLAGGED", "INTERMITTENT"]))
                    .groupby("state", dropna=True)
                    .agg(
                        stations=("station", "count"),
                        clean=("status", lambda s: (s == "CLEAN").sum()),
                        flagged=("status", lambda s: (s == "FLAGGED").sum()),
                        missing=("status", lambda s: s.isin(
                            ["MISSING", "NO DATA"]).sum()),
                        bad=("bad", "sum"),
                    )
                    .reset_index().sort_values("bad", ascending=False)
                )
                by_state["health_pct"] = (100.0 * by_state["clean"] / by_state["stations"]).round(1)
                rename = {
                    "state": "ST", "stations": "Stations", "clean": "Clean",
                    "flagged": "Flagged", "missing": "Missing",
                    "bad": "Needs Attn", "health_pct": "Health %",
                }
                _grid(by_state[list(rename)].rename(columns=rename),
                      height=460, key="fcst_by_state", pinned=["ST"],
                      status_col=None)
        else:
            st.error("AOMC catalog not loaded.")

    # ---- D. Active NWS CAP alerts -------------------------------------
    with fcst_d:
        st.markdown("**Active NWS alerts** (api.weather.gov)")
        if _HAVE_CAPS:
            with st.spinner("Fetching active alerts…"):
                alerts = _caps_active(fcst_ck)
            st.caption(f"{len(alerts):,} active alerts nationally.")
            if alerts:
                # Filter
                events = sorted({a.get("event") for a in alerts if a.get("event")})
                sel_events = st.multiselect(
                    "Event type filter", events,
                    default=[e for e in events if "Warning" in e or "Tornado" in e][:5],
                    key="fcst_caps_ev",
                )
                sevs = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]
                sel_sevs = st.multiselect(
                    "Severity filter", sevs, default=["Extreme", "Severe"],
                    key="fcst_caps_sev",
                )
                shown = [a for a in alerts
                         if (not sel_events or a.get("event") in sel_events)
                         and (not sel_sevs or a.get("severity") in sel_sevs)]
                rows = []
                for a in shown[:400]:
                    rows.append({
                        "Event": a.get("event") or "",
                        "Severity": a.get("severity") or "Unknown",
                        "Urgency": a.get("urgency") or "",
                        "Area": (a.get("area_desc") or "")[:100],
                        "Sent":  str(a.get("sent") or ""),
                        "Expires": str(a.get("expires") or ""),
                        "Sender": a.get("sender") or "",
                    })
                _grid(pd.DataFrame(rows), height=500, key="fc_caps",
                      pinned=["Event"], status_col="Severity")
        else:
            st.error("CAP alerts client not installed.")

    # ---- E. Space Weather (NOAA SWPC) ----------------------------------
    with fcst_e:
        st.markdown("**Live Space Weather**  (NOAA SWPC)")
        _section_help(
            "About Space Weather",
            what=(
                "Live geomagnetic + solar conditions pulled directly from "
                "NOAA's Space Weather Prediction Center (SWPC). Three "
                "datasets update continuously: **planetary Kp index** "
                "(3-hour cadence, 0-9 scale — geomagnetic storm level), "
                "**GOES X-ray flux** (1-minute, solar-flare classification "
                "A/B/C/M/X), and **active SWPC alerts** (issued as events "
                "warrant). All zero-auth federal data."
            ),
            who=[
                "**NWS aviation forecasters** — incorporate storm-level Kp into overnight discussions for transoceanic ops.",
                "**ATC supervisors** (especially Oakland / New York / Anchorage Oceanic) — HF comms degrade when Kp >= 5. This is the early-warning banner.",
                "**Field maintenance techs** — remote Alaskan / Pacific ASOS rely on HF radio; crews on truck rolls watch this before departing.",
                "**NOC duty officers** — unusual ASOS error patterns in Alaska sometimes correlate with geomagnetic storms affecting GPS-timed comms.",
            ],
            how=[
                "Glance at **Kp** — under 5 is routine, 5-6 is watch-worthy, 7-9 triggers radio-blackout advisories.",
                "Check **X-ray class** — M-class flares cause minor HF blackouts, X-class causes major.",
                "Scroll the **alerts** list for SWPC-issued G/R/S-scale watches and warnings; each message is verbatim from the forecasters at SWPC.",
                "Cross-reference a Kp spike with your Alaska / Hawaii station MISSING rate — if several go silent during a G3+ storm, that's likely comms not sensor failure.",
            ],
            output=(
                "NOAA G-scale (geomagnetic): G1 minor (Kp=5) -> G5 extreme "
                "(Kp=9). R-scale (radio blackouts): R1 minor (M1 flare) -> "
                "R5 extreme (X20+ flare). S-scale (solar radiation): S1 -> "
                "S5. Each has well-defined operational impacts — see "
                "swpc.noaa.gov/noaa-scales-explanation."
            ),
        )
        try:
            from asos_tools.space_weather import space_weather_summary
            sw = space_weather_summary()
        except Exception:
            logger.exception("space weather fetch failed")
            sw = None

        if not sw:
            st.info("Space weather data temporarily unavailable.")
        else:
            kp = sw.get("kp") or {}
            xr = sw.get("xray") or {}
            alerts = sw.get("alerts") or []

            kc1, kc2, kc3 = st.columns(3)
            kp_val = kp.get("kp")
            kp_label = kp.get("label", "unknown")
            kp_color = (
                "#dc2626" if (kp_val or 0) >= 7 else
                "#f59e0b" if (kp_val or 0) >= 5 else
                "#22c55e"
            )
            kc1.markdown(
                f"<div style='padding:12px 16px;background:rgba(0,0,0,0.02);"
                f"border-left:4px solid {kp_color};border-radius:4px;'>"
                f"<div style='font-size:10px;letter-spacing:0.1em;"
                f"color:#64748b;text-transform:uppercase;'>Planetary Kp</div>"
                f"<div style='font-size:28px;font-weight:700;color:{kp_color};'>"
                f"{kp_val if kp_val is not None else '-'}</div>"
                f"<div style='font-size:11px;color:#334155;'>{kp_label}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            xr_class = xr.get("class", "-")
            xr_color = (
                "#dc2626" if xr_class and xr_class.startswith("X") else
                "#f59e0b" if xr_class and xr_class.startswith("M") else
                "#22c55e"
            )
            kc2.markdown(
                f"<div style='padding:12px 16px;background:rgba(0,0,0,0.02);"
                f"border-left:4px solid {xr_color};border-radius:4px;'>"
                f"<div style='font-size:10px;letter-spacing:0.1em;"
                f"color:#64748b;text-transform:uppercase;'>Solar X-ray</div>"
                f"<div style='font-size:28px;font-weight:700;color:{xr_color};'>"
                f"{xr_class or '-'}</div>"
                f"<div style='font-size:11px;color:#334155;'>"
                f"GOES primary · long channel</div></div>",
                unsafe_allow_html=True,
            )
            kc3.markdown(
                f"<div style='padding:12px 16px;background:rgba(0,0,0,0.02);"
                f"border-left:4px solid #38bdf8;border-radius:4px;'>"
                f"<div style='font-size:10px;letter-spacing:0.1em;"
                f"color:#64748b;text-transform:uppercase;'>Active Alerts</div>"
                f"<div style='font-size:28px;font-weight:700;color:#38bdf8;'>"
                f"{len(alerts)}</div>"
                f"<div style='font-size:11px;color:#334155;'>SWPC feed</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            if alerts:
                st.markdown("**Recent SWPC alerts**")
                for a in alerts:
                    with st.expander(f"{a.get('id','?')} - {a.get('time_utc','')}"):
                        st.text(a.get("message", ""))
            st.caption(
                "Source: services.swpc.noaa.gov - Kp 3-hour, X-ray 1-min, "
                "alerts event-driven. Cached 3 min."
            )


# ===========================================================================
# ADMIN — alerts, scheduler, cache, anomaly detection (auth-gated)
# ===========================================================================

with tab_admin:
    st.markdown("### Administrator Console")
    st.caption(
        "System operations: alert routing, background scheduler, "
        "persistent cache, anomaly detection."
    )
    _section_help(
        "About the Admin tab",
        what=(
            "Operator-level controls that affect the whole app, not just a "
            "single station. Five sub-tabs partition the responsibilities: "
            "**Alerts** (notification routing), **Scheduler** (background "
            "scans), **Cache** (data freshness + storage), **Anomaly "
            "Detection** (per-station deep analysis), and **Data Sources** "
            "(every upstream feed O.W.L. consumes)."
        ),
        who=[
            "**System administrators** — primary audience; own the service health.",
            "**Ops engineers / DevOps** — watch scheduler, cache stats, and tick metrics here.",
            "**Incident responders** — use Anomaly Detection to find the precursor to a reported issue.",
            "**Compliance auditors** — use Data Sources to prove every data point has a federal / authoritative origin.",
        ],
        how=[
            "Walk the sub-tabs left-to-right: Alerts -> Scheduler -> Cache -> Anomaly -> Data Sources.",
            "Each sub-tab has its own 'What this is + who uses it' expander inside.",
            "Changes here take effect on next scan (cache) or next session (alerts).",
        ],
    )

    if True:
        admin_tabs = st.tabs([
            "Alerts", "Scheduler", "Cache", "Anomaly Detection", "Data Sources",
        ])

        # -- Alerts ------------------------------------------------------
        with admin_tabs[0]:
            _section_help(
                "About Alerts",
                what=(
                    "Configure where O.W.L. sends notifications when a "
                    "watchlist scan detects a newly-flagged or missing "
                    "station. Uses the Apprise library, which speaks 70+ "
                    "downstream services (Slack, Discord, Teams, email, "
                    "SMS via Twilio, PagerDuty, Pushover, Telegram, etc.) "
                    "via a unified URL scheme."
                ),
                who=[
                    "**NOC team leads** — wire Slack or Teams so shift members get a ping when a new `$` fires.",
                    "**On-call engineers** — add a PagerDuty URL for severity-Severe CAP alerts.",
                    "**Solo controllers** — use Pushover or SMS for after-hours alerts.",
                ],
                how=[
                    "Set the `OWL_ALERT_URLS` env var on the HF Space (comma-separated Apprise URLs).",
                    "Use the **Send test alert** button to verify wiring end-to-end before the next scan fires.",
                    "Rotate or disable channels by updating the env var and restarting the Space.",
                ],
                output=(
                    "One alert per newly-flagged station per scan. Missing "
                    "stations get a distinct MISSING alert. No spam — the "
                    "circuit breaker suppresses the bulk-alert flood when "
                    "the whole fleet classifies as MISSING (upstream outage)."
                ),
            )
            st.markdown("#### Notification routing (Apprise)")
            st.caption(
                "Set the `OWL_ALERT_URLS` environment variable "
                "(comma-separated) to route alerts to Slack, Discord, "
                "Teams, email, PagerDuty, webhooks, and more."
            )
            if _HAVE_APPRISE:
                urls = owl_alerts.load_urls_from_env()
                if urls:
                    st.success(f"{len(urls)} recipient URL(s) configured.")
                    for u in urls:
                        # Hide credentials in the display.
                        masked = u.split("://")[0] + "://***"
                        st.code(masked, language="")
                else:
                    st.warning("No recipients configured. Set `OWL_ALERT_URLS`.")

                with st.expander("Supported services & URL formats"):
                    for name, template in owl_alerts.SUPPORTED_SERVICES.items():
                        st.markdown(f"**{name}** — `{template}`")

                if st.button("Send test alert", key="adm_test_alert",
                             type="primary"):
                    sent, failed = owl_alerts.send_test_alert()
                    if sent:
                        st.success(f"Test alert delivered to {sent} recipient(s).")
                    elif failed:
                        st.error(f"All {failed} deliveries failed. "
                                 "Check the URLs and logs.")
                    else:
                        st.info("No URLs configured. Set `OWL_ALERT_URLS` first.")
            else:
                st.error("Apprise is not installed.")

        # -- Scheduler ---------------------------------------------------
        with admin_tabs[1]:
            st.markdown("#### Background scheduler (APScheduler)")
            st.caption(
                "Refreshes the whole-network watchlist every 3 minutes so "
                "the Summary tab loads instantly."
            )
            _section_help(
                "About Scheduler",
                what=(
                    "Two scheduling layers run in parallel: (a) an optional "
                    "in-container **APScheduler** that refreshes the "
                    "watchlist periodically and pushes results into the "
                    "DiskCache, and (b) an external **GitHub Actions cron** "
                    "that POSTs `/api/tick` every 5 minutes and survives "
                    "container restarts. The external cron is the primary; "
                    "APScheduler is legacy and disabled unless "
                    "`OWL_ENABLE_BG_REFRESH=1`."
                ),
                who=[
                    "**Ops engineers** — watch tick cadence, p50/p95 duration, failure counts.",
                    "**Admins setting up a new deploy** — confirm the external cron secret is in place and the first tick landed green.",
                    "**Incident responders after an outage** — check `upstream_outage` flag and last_error to see why scans stopped.",
                ],
                how=[
                    "The external cron is already configured via `.github/workflows/owl-tick.yml` — just keep `OWL_CRON_SECRET` synced between HF Space secrets and GitHub repo secrets.",
                    "To enable the in-container scheduler additionally, set `OWL_ENABLE_BG_REFRESH=1` on the HF Space.",
                    "Use `/api/health` (or the metrics shown below) to verify tick frequency and success rate.",
                ],
                output=(
                    "The metrics panel below shows total/ok/failed/overlap "
                    "tick counts and p50/p95 scan duration. A healthy "
                    "deployment is >98% ok rate with p95 < 30s (AWC primary)."
                ),
            )
            if _HAVE_SCHED:
                status = scheduler_status()
                if not status.get("available"):
                    st.error(status.get("reason") or "Scheduler unavailable.")
                else:
                    cc1, cc2, cc3 = st.columns(3)
                    cc1.metric("Running", "YES" if status.get("running") else "NO")
                    cc2.metric("Jobs", len(status.get("jobs", [])))
                    cc3.metric("Engine", "APScheduler")
                    for j in status.get("jobs", []):
                        st.markdown(f"**{j.get('id')}**")
                        st.caption(f"Trigger: {j.get('trigger')}")
                        st.caption(f"Next run: {j.get('next_run') or '—'}")
                        st.caption(f"Last run: {j.get('last_run') or '—'}")
                        if j.get("last_error"):
                            st.error(f"Last error: {j['last_error']}")
                        else:
                            st.success("Last run healthy.")
            else:
                st.error("APScheduler is not installed.")

        # -- Cache -------------------------------------------------------
        with admin_tabs[2]:
            st.markdown("#### Persistent cache (DiskCache)")
            st.caption(
                "Watchlist scans are cached to disk so they survive Space "
                "restarts and kick-starts after code pushes."
            )
            _section_help(
                "About Cache",
                what=(
                    "A SQLite-backed key-value store (DiskCache) used to "
                    "persist the most recent whole-network watchlist scan. "
                    "Without it every page load would trigger a fresh AWC "
                    "fetch; with it, repeat visits and multi-user views "
                    "render in milliseconds from cached data."
                ),
                who=[
                    "**All users** benefit indirectly — the cache is why the Summary tab loads instantly after a warm scan.",
                    "**Admins** watch the hit/miss rate and disk usage to decide if the $5/mo Persistent Storage add-on is worthwhile.",
                    "**Incident responders** — can force-clear the cache to rule it out as a cause of stale data.",
                ],
                how=[
                    "The cache dir defaults to `/tmp/owl-cache` (ephemeral) or `/data/cache` if the HF Persistent Storage add-on is enabled.",
                    "Click **Clear cache** to evict everything — next scan repopulates.",
                    "Size limit is 200 MB (LRU eviction).  Hit rate in a warm cache is typically >95%.",
                ],
                output=(
                    "Three numbers tell you if the cache is healthy: **hit "
                    "rate** (>85% is fine), **items** (~1-10 in steady state "
                    "— we only cache per-session watchlist snapshots), and "
                    "**size MB** (well under the 200 MB cap)."
                ),
            )
            if _HAVE_PC:
                stats = _pc_stats()
                if stats.get("available"):
                    cc1, cc2, cc3, cc4 = st.columns(4)
                    cc1.metric("Items", stats.get("items", 0))
                    cc2.metric("Size (MB)", f"{stats.get('size_mb', 0):.2f}")
                    cc3.metric("Hit rate",
                               f"{stats.get('hit_rate', 0):.1f}%")
                    cc4.metric("Hits / misses",
                               f"{stats.get('hits', 0)} / {stats.get('misses', 0)}")
                    st.caption(f"Directory: `{stats.get('directory', '?')}`")
                    st.caption(f"Snapshot at: {stats.get('as_of', '?')}")
                    if st.button("Clear cache", key="adm_cache_clear"):
                        n = _pc_clear()
                        st.success(f"Cleared {n} cached item(s).")
                else:
                    st.error(stats.get("reason")
                             or stats.get("error")
                             or "Cache unavailable.")
            else:
                st.error("DiskCache is not installed.")

        # -- Anomaly Detection -------------------------------------------
        with admin_tabs[3]:
            st.markdown("#### Anomaly detection (STUMPY Matrix Profile)")
            st.caption(
                "Find the most unusual subsequence in a single station's "
                "1-minute time series — surfaces sensor drift or data "
                "glitches that $ alone wouldn't catch."
            )
            with st.expander("What this is + who it's for", expanded=False):
                st.markdown(
                    """
**Function.** Pulls the last 1–14 days of 1-minute data for one
station, selects one sensor channel (temperature, dewpoint, wind, or
pressure), and surfaces the **most statistically unusual 15–120
minute window** in the series. Discord scoring is symmetric to the
rest of the time series — a window with no close analogue anywhere
else is flagged, regardless of its absolute magnitude.

**The math.** *Matrix Profile* computes, for every rolling window in
the series, the distance to its nearest lookalike elsewhere in the
*same* series. A window whose nearest neighbor is *far away* is
called a **discord** — a pattern that appears nowhere else. Highest
score = most unusual. **STUMPY** is the Python library that computes
this fast (14-day, 1-min series in <1 sec).

**Why it matters.** The `$` maintenance flag only fires *after* the
ASOS fails an internal tolerance check. Matrix Profile catches
statistical weirdness *before* the threshold trips — earlier warning
than `$`, and with no false positives during normal diurnal cycles.

---

**Say for:**

- **AOMC controllers** — catch a drifting temp probe before it hits
  tolerance and triggers `$`, or explain why a station has been
  `INTERMITTENT` for the last two days.
- **Field maintenance techs** — prioritize truck rolls by picking
  the station with the highest anomaly score this week rather than
  visiting in alphabetical order.
- **NWS forecasters** — sanity-check an outlier METAR before citing
  it in a discussion ("is 98°F at 03 UTC plausible, or is KXYZ's
  temp sensor reading is implausible and stuck").
- **QA analysts** — audit a month of historical 1-min data for
  calibration drift when comparing two nearby stations.
- **Incident investigators** — after a `$` event or missing METAR,
  look back 72 hours to find the subtle anomaly that preceded it,
  then attach the plot to the incident report.
- **Research users** — benchmark "normal" diurnal patterns per
  season / per biome and spot climate-signal anomalies.

**How to read the output:**

| Score | Meaning |
|---|---|
| `> 6` | Genuinely weird — worth investigating |
| `3 – 6` | Possibly interesting — context-dependent |
| `< 3` | Statistical noise — probably nothing |
                    """,
                    unsafe_allow_html=False,
                )
            if _HAVE_ANOMALY:
                ac1, ac2, ac3 = st.columns([1, 1, 1])
                with ac1:
                    if _HAVE_CATALOG:
                        pool_ids = [s["id"] for s in AOMC_STATIONS if s.get("id")]
                        _plk2 = _AOMC_META
                        a_sid = st.selectbox(
                            "Station", pool_ids,
                            index=pool_ids.index("KJFK") if "KJFK" in pool_ids else 0,
                            format_func=lambda s: (
                                f"{s} · {_short_name(_plk2.get(s, {}).get('name', ''))}"
                            ),
                            key="an_sid",
                        )
                    else:
                        a_sid = st.text_input("ICAO ID", "KJFK",
                                              key="an_sid_txt").strip().upper()
                with ac2:
                    a_col = st.selectbox(
                        "Column",
                        ["temp_2m_f", "dew_point_f", "wind_speed_2m_mph",
                         "pressure_hg"],
                        key="an_col",
                    )
                with ac3:
                    a_win = st.selectbox(
                        "Window (min)", [15, 30, 60, 120],
                        index=1, key="an_win",
                    )
                a_days = st.slider("Lookback (days)", 1, 14, 3, key="an_days")

                if st.button("Scan for anomalies", key="an_run",
                             type="primary"):
                    end_a = datetime.now(timezone.utc)
                    start_a = end_a - timedelta(days=a_days)
                    with st.spinner(f"Fetching {a_sid} and running matrix profile…"):
                        df_a = _fetch_1min(a_sid, start_a, end_a)
                        if df_a.empty:
                            st.error("No 1-min data for this station/window.")
                        else:
                            result = detect_anomalies(
                                df_a, column=a_col, window_minutes=a_win,
                            )
                            if not result.has_anomaly:
                                st.info(
                                    "No significant anomalies detected. "
                                    "Try a longer lookback or a different column."
                                )
                            else:
                                rc1, rc2, rc3 = st.columns(3)
                                rc1.metric("Score", f"{result.discord_score:.2f}")
                                rc2.metric("Window",
                                           f"{result.window_minutes} min")
                                rc3.metric("Observations",
                                           f"{result.n_points:,}")
                                if result.discord_time is not None:
                                    st.caption(
                                        f"Most unusual period starts at "
                                        f"**{result.discord_time:%Y-%m-%d %H:%MZ}** "
                                        f"({result.column})."
                                    )
                                if result.top_k_times:
                                    st.markdown("**Top-k anomalies**")
                                    tk = pd.DataFrame({
                                        "Rank": range(1, len(result.top_k_scores) + 1),
                                        "Start (UTC)": result.top_k_times,
                                        "Score": [f"{s:.2f}"
                                                  for s in result.top_k_scores],
                                    })
                                    st.dataframe(_arrow_safe(tk),
                                                 use_container_width=True,
                                                 hide_index=True)
            else:
                st.error("STUMPY is not installed.")

        # -- Data Sources --------------------------------------------------
        with admin_tabs[4]:
            st.markdown("#### Upstream data sources")
            st.caption(
                "Every number shown anywhere in O.W.L. traces back to "
                "one of these public feeds. Click to visit the source."
            )
            _section_help(
                "About Data Sources",
                what=(
                    "The public registry of every upstream feed O.W.L. "
                    "consumes, with their trust tier (federal authoritative / "
                    "federal mirror / academic) and update cadence. Used for "
                    "transparency — any statistic in the app can be traced "
                    "back to a source row here."
                ),
                who=[
                    "**Compliance auditors** — prove every number has a federal / authoritative origin.",
                    "**Users who want to verify a specific METAR** — click through to the source feed for the raw data.",
                    "**Admins evaluating outage impact** — see at a glance which features depend on which upstream source.",
                    "**Researchers citing O.W.L. data** — get the exact URL for the footnote.",
                ],
                how=[
                    "Browse the table — rows are ordered by how critical the source is (METAR > News > Webcams).",
                    "Click a URL to open the source's landing page.",
                    "Cross-reference the **Used for** column with whichever O.W.L. feature you're auditing.",
                ],
                output=(
                    "Three trust tiers: **federal** (the authoritative "
                    "source, usually NOAA/NWS/FAA/NCEI); **mirror** (a "
                    "reliable non-federal copy, e.g. IEM); **academic** "
                    "(university research services). No commercial or "
                    "scraped sources are in the pipeline."
                ),
            )
            if _HAVE_SOURCES and DATA_SOURCES:
                src_rows = []
                for s in DATA_SOURCES:
                    src_rows.append({
                        "Source": s.get("name", ""),
                        "Trust": (s.get("trust", "") or "").title(),
                        "Used for": s.get("used_for", ""),
                        "Auth": s.get("auth", ""),
                        "Cadence": s.get("cadence", ""),
                        "URL": s.get("url", ""),
                        "Notes": s.get("notes", ""),
                    })
                _grid(pd.DataFrame(src_rows), height=500, key="adm_sources",
                      pinned=["Source"], status_col="Trust")
            else:
                st.warning("Source registry not loaded.")

        st.divider()
        st.caption(
            "For gated deployments, front the service with an authenticated "
            "reverse proxy (Entra / Okta / Login.gov via OIDC, or CAC/PIV "
            "via mod_ssl client-certificates). Settings and alerts written "
            "here take effect on the next scan."
        )


# ===========================================================================
# FEDERAL FOOTER — citations, source chain, system metadata
# ===========================================================================

_now_footer = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
_footer_html = (
    '<div class="fed-footer">'
    '<div class="fed-footer-cite">'
    '<strong>Data Source Chain:</strong> '
    'NOAA / NCEI ASOS METAR archive &rsaquo; '
    'Iowa Environmental Mesonet (mesonet.agron.iastate.edu) &rsaquo; '
    'O.W.L. processing layer. '
    '<strong>Station Catalog:</strong> NCEI Historical Observing Metadata Repository '
    f'(HOMR) asos-stations.txt &mdash; {len(AOMC_STATIONS):,} federally-operated AOMC '
    'stations (NWS / FAA / DOD).'
    '</div>'
    '<div class="fed-footer-meta">'
    f'O.W.L. &mdash; OBSERVATION WATCH LOG &middot; v1.1.0 &middot; SYSTEM TIME {_now_footer} &middot; '
    '<a href="https://github.com/consigcody94/asos-tools-py">SOURCE</a> &middot; '
    '<a href="https://www.ncei.noaa.gov">NCEI</a> &middot; '
    '<a href="https://www.weather.gov/asos/asostech">ASOS DOCS</a> &middot; '
    '<a href="https://mesonet.agron.iastate.edu">IEM</a>'
    '</div>'
    '</div>'
)
st.html(_footer_html)
