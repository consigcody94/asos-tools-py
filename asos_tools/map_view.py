"""Interactive Folium map of AOMC stations, colored by watchlist status.

Every AOMC station has ``lat``/``lon`` populated from NCEI HOMR; we drop
them on a Folium map using CircleMarkers colored by status:

    MISSING / NO DATA  -> red     (silent)
    FLAGGED            -> amber   ($ maintenance indicator)
    INTERMITTENT       -> orange  (mixed)
    RECOVERED          -> blue    (recently back)
    CLEAN              -> green   (healthy)

Marker popups include station ID, name, state, current status, and the
latest METAR so controllers can triage without leaving the map.
"""

from __future__ import annotations

from typing import Iterable, Mapping, Optional

import folium
import pandas as pd
from folium.plugins import MarkerCluster

__all__ = ["build_status_map", "STATUS_COLORS"]


#: Color per watchlist status. Matches USWDS semantic colors.
STATUS_COLORS: dict[str, str] = {
    "MISSING":      "#b50909",   # USWDS error red
    "NO DATA":      "#7f1d1d",   # darker red — no data at all in window
    "FLAGGED":      "#ffbe2e",   # USWDS warning yellow
    "INTERMITTENT": "#f97316",   # orange — mixed
    "RECOVERED":    "#38bdf8",   # light blue — recently back
    "CLEAN":        "#00a91c",   # USWDS success green
}

#: Icon size in px per status so critical stations pop visually.
STATUS_RADIUS: dict[str, int] = {
    "MISSING":      7,
    "NO DATA":      7,
    "FLAGGED":      6,
    "INTERMITTENT": 6,
    "RECOVERED":    4,
    "CLEAN":        3,
}


def _popup_html(row: Mapping) -> str:
    """HTML body for the marker popup."""
    st = row.get("station", "") or ""
    nm = (row.get("name") or "").title()
    state = row.get("state") or ""
    status = row.get("status") or ""
    reason = row.get("probable_reason") or "—"
    flagged = row.get("flagged", 0)
    total = row.get("total", 0)
    rate = row.get("flag_rate")
    latest = row.get("latest_metar") or ""
    color = STATUS_COLORS.get(status, "#64748b")

    rate_txt = f"{rate:.0f}%" if isinstance(rate, (int, float)) and rate else "—"

    return (
        f'<div style="font-family:Inter,sans-serif;font-size:12px;min-width:260px;max-width:360px;">'
        f'<div style="font-weight:700;font-size:14px;margin-bottom:2px;color:#0f172a;">'
        f'{st} &middot; {nm}</div>'
        f'<div style="color:#64748b;margin-bottom:6px;">{state}</div>'
        f'<div style="margin-bottom:4px;"><span style="background:{color};color:#fff;'
        f'padding:2px 7px;border-radius:3px;font-weight:700;letter-spacing:0.08em;'
        f'font-size:10px;">{status}</span></div>'
        f'<div style="margin-top:6px;"><b>Reason:</b> {reason}</div>'
        f'<div><b>Flagged:</b> {flagged} / {total} ({rate_txt})</div>'
        f'<div style="margin-top:6px;font-family:JetBrains Mono,monospace;'
        f'font-size:10.5px;color:#334155;background:#f1f5f9;padding:6px 8px;'
        f'border-radius:4px;word-break:break-word;">{latest or "(no METAR)"}</div>'
        f'</div>'
    )


def build_status_map(
    watchlist_df: pd.DataFrame,
    aomc_stations: Iterable[Mapping],
    *,
    cluster: bool = True,
    center: Optional[tuple[float, float]] = None,
    zoom: int = 4,
    dark: bool = False,
    height_px: int = 520,
) -> folium.Map:
    """Return a Folium map with each station colored by watchlist status.

    Parameters
    ----------
    watchlist_df
        Output of :func:`build_watchlist`; must contain ``station`` and
        ``status`` columns.
    aomc_stations
        Iterable of station metadata dicts (needs ``id``, ``lat``, ``lon``,
        ``name``, ``state``). Typically ``AOMC_STATIONS``.
    cluster
        If True, use MarkerCluster for performance with ~900 markers.
    center, zoom
        Map viewport. Default center is CONUS.
    dark
        Use a dark Carto basemap instead of OpenStreetMap.
    """
    wl_map = {r["station"]: r for _, r in watchlist_df.iterrows()} \
        if not watchlist_df.empty else {}

    center = center or (39.0, -98.5)  # CONUS center
    tiles = "CartoDB dark_matter" if dark else "CartoDB positron"
    m = folium.Map(
        location=center,
        zoom_start=zoom,
        tiles=tiles,
        control_scale=True,
        prefer_canvas=True,
        height=height_px,
    )

    group = MarkerCluster(name="AOMC stations",
                          options={"disableClusteringAtZoom": 6}) if cluster \
        else folium.FeatureGroup(name="AOMC stations")

    for s in aomc_stations:
        sid = s.get("id")
        lat = s.get("lat")
        lon = s.get("lon")
        if sid is None or lat is None or lon is None:
            continue
        row = wl_map.get(sid, {})
        status = row.get("status") or "CLEAN"  # default if no scan data
        color = STATUS_COLORS.get(status, "#64748b")
        radius = STATUS_RADIUS.get(status, 3)
        popup = folium.Popup(
            _popup_html({**s, **row, "station": sid}),
            max_width=400,
        )
        folium.CircleMarker(
            location=(lat, lon),
            radius=radius,
            color=color,
            weight=1.2,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            popup=popup,
            tooltip=f"{sid} · {status}",
        ).add_to(group)

    group.add_to(m)

    # Legend (pure HTML overlay).
    legend_items = [
        ("MISSING / NO DATA", STATUS_COLORS["MISSING"]),
        ("FLAGGED ($)", STATUS_COLORS["FLAGGED"]),
        ("INTERMITTENT", STATUS_COLORS["INTERMITTENT"]),
        ("RECOVERED", STATUS_COLORS["RECOVERED"]),
        ("CLEAN", STATUS_COLORS["CLEAN"]),
    ]
    text = "#0f172a" if not dark else "#f1f5f9"
    bg = "#ffffff" if not dark else "#0f172a"
    legend_html = (
        f'<div style="position:fixed;bottom:24px;right:24px;z-index:9999;'
        f'background:{bg};padding:10px 14px;border-radius:8px;'
        f'box-shadow:0 4px 14px rgba(15,23,42,0.2);'
        f'font-family:Inter,sans-serif;font-size:11px;color:{text};">'
        f'<div style="font-weight:700;letter-spacing:0.1em;'
        f'text-transform:uppercase;font-size:10px;margin-bottom:6px;">'
        f'Status</div>'
    )
    for label, color in legend_items:
        legend_html += (
            f'<div style="display:flex;align-items:center;gap:6px;'
            f'margin:3px 0;">'
            f'<span style="width:10px;height:10px;border-radius:50%;'
            f'background:{color};display:inline-block;"></span>'
            f'{label}</div>'
        )
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))

    return m
