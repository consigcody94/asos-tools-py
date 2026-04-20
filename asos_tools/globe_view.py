"""3D satellite globe for the O.W.L. Summary dashboard.

Replaces the Folium 2D map with a Globe.gl (three.js) WebGL globe so:

* it's literally a sphere — tiles physically cannot repeat at any zoom
* satellite-textured (NASA Blue Marble), not a road map
* points are 3D glowing dots sized + colored by station status
* atmosphere glow + auto-rotate on first load
* click a point -> postMessage back to Streamlit, which switches tabs
* runs entirely client-side (CDN libs), no server round-trip per render

Implementation
--------------
A single function :func:`build_globe_html` returns a self-contained HTML
string (no external state). Streamlit hosts it via
``streamlit.components.v1.html``.

The globe loads three CDN modules:

* ``three`` v0.160 (WebGL renderer)
* ``globe.gl`` v2.32 (Vasco Asturiano's three.js wrapper)
* nothing else — the rest is vanilla JS + a small CSS overlay

Performance budget (per the plan):
* Initial bundle: ~450 KB JS from CDN (browser-cached after first load)
* Texture: ~1 MB Blue Marble (cached)
* 920 station points render at 60 fps on mid-range hardware
* Click handler is event-delegated (no per-point listener)

Click contract:
When a user clicks a point, the iframe posts a message to its parent:

    window.parent.postMessage(
        {"type": "owl.station.click", "station": "KJFK"},
        "*"
    )

The Streamlit page can listen via a tiny JS shim (added separately in
app.py wiring) and switch to the Reports tab with that station preselected.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

import pandas as pd

__all__ = ["build_globe_html", "STATUS_COLORS"]


# Color per status — picked for high contrast on a dark globe.
# Order matches watchlist.STATUS_ORDER.
STATUS_COLORS: dict[str, str] = {
    "MISSING":      "#dc2626",  # red-600
    "FLAGGED":      "#f59e0b",  # amber-500
    "INTERMITTENT": "#eab308",  # yellow-500
    "RECOVERED":    "#06b6d4",  # cyan-500
    "CLEAN":        "#22c55e",  # green-500
    "NO DATA":      "#64748b",  # slate-500
}

# Point altitude (sphere-radius units) per status — flagged stations sit
# slightly above the surface so they're visually prominent.
_STATUS_ALTITUDE: dict[str, float] = {
    "MISSING":      0.024,
    "FLAGGED":      0.020,
    "INTERMITTENT": 0.016,
    "RECOVERED":    0.008,
    "CLEAN":        0.004,
    "NO DATA":      0.004,
}

# Point radius per status (Three-Globe units).
_STATUS_RADIUS: dict[str, float] = {
    "MISSING":      0.55,
    "FLAGGED":      0.45,
    "INTERMITTENT": 0.40,
    "RECOVERED":    0.30,
    "CLEAN":        0.22,
    "NO DATA":      0.22,
}


def _watchlist_to_points(
    watchlist_df: Optional[pd.DataFrame],
    station_meta: Optional[Iterable[dict]] = None,
) -> list[dict]:
    """Convert a watchlist DataFrame into Globe.gl-friendly point dicts.

    Joins ``watchlist_df`` (which has ``station, status, ...``) against
    ``station_meta`` (AOMC catalog dicts with ``id, name, state, lat,
    lon``) to produce the lat/lon needed for plotting.
    """
    if watchlist_df is None or watchlist_df.empty:
        return []

    meta = {m["id"]: m for m in (station_meta or []) if m.get("id")}
    points: list[dict] = []
    for _, row in watchlist_df.iterrows():
        sid = str(row.get("station") or "")
        if not sid:
            continue
        m = meta.get(sid, {})
        lat = m.get("lat") or row.get("lat")
        lon = m.get("lon") or row.get("lon")
        if lat is None or lon is None:
            continue
        try:
            lat = float(lat)
            lon = float(lon)
        except Exception:
            continue
        status = str(row.get("status") or "NO DATA").upper()
        points.append({
            "station":    sid,
            "name":       (m.get("name") or row.get("name") or "").title(),
            "state":      m.get("state") or row.get("state") or "",
            "lat":        lat,
            "lon":        lon,
            "status":     status,
            "color":      STATUS_COLORS.get(status, STATUS_COLORS["NO DATA"]),
            "alt":        _STATUS_ALTITUDE.get(status, 0.004),
            "radius":     _STATUS_RADIUS.get(status, 0.22),
            "reason":     row.get("probable_reason") or "",
            "latest_metar": str(row.get("latest_metar") or "")[:200],
        })
    return points


def build_globe_html(
    watchlist_df: Optional[pd.DataFrame] = None,
    *,
    station_meta: Optional[Iterable[dict]] = None,
    height_px: int = 640,
    auto_rotate: bool = True,
    dark: bool = True,
    show_atmosphere: bool = True,
    starfield: bool = True,
    news_items: Optional[list[dict]] = None,
    radar_overlay_url: Optional[str] = None,
    satellite_overlay_url: Optional[str] = None,
) -> str:
    """Return a self-contained HTML document for the 3D globe.

    Parameters
    ----------
    watchlist_df
        Output of :func:`asos_tools.watchlist.build_watchlist`.
        If empty/None, the globe still renders with no points.
    station_meta
        AOMC catalog rows (with ``id, name, state, lat, lon``) used to
        join lat/lon onto the watchlist by station id.
    height_px
        Height of the embedded iframe in pixels.
    auto_rotate
        Whether the globe spins slowly until the user interacts.
    dark
        ``True`` -> Blue Marble (day-night composite); ``False`` -> daytime.
    show_atmosphere
        ``True`` enables the cyan halo around the globe (Globe.gl built-in).
    starfield
        ``True`` adds a black-with-stars background.
    """
    points = _watchlist_to_points(watchlist_df, station_meta)

    # JSON-embed the points so the JS template doesn't need a fetch round-trip.
    # Limit to 1500 points just in case (the watchlist is normally 920).
    points_json = json.dumps(points[:1500], separators=(",", ":"))

    # Texture choice — Blue Marble has both day + night composites at the
    # same URL (provided by the globe.gl examples bucket; CDN-hosted, free).
    earth_texture = (
        "//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
        if dark else
        "//unpkg.com/three-globe/example/img/earth-day.jpg"
    )
    bump_texture = "//unpkg.com/three-globe/example/img/earth-topology.png"
    night_texture = "//unpkg.com/three-globe/example/img/night-sky.png"

    bg_color = "#000000" if dark else "#0b1220"
    atmosphere_color = "#38bdf8"
    atmosphere_alt = 0.18 if show_atmosphere else 0.0

    # We pre-stringify config to keep the template literal-free.
    config_json = json.dumps({
        "earth_texture":      earth_texture,
        "bump_texture":       bump_texture,
        "night_texture":      night_texture,
        "bg_color":           bg_color,
        "atmosphere_color":   atmosphere_color,
        "atmosphere_alt":     atmosphere_alt,
        "auto_rotate":        bool(auto_rotate),
        "dark":               bool(dark),
        "starfield":          bool(starfield),
        "height_px":          int(height_px),
        # Overlay layers — optional; when set, rendered as a HTML/CSS
        # plane over the globe canvas.  Mesh-projected is hard without
        # cesium-terrain-tiles; a 2D pinned overlay is 90% of the value
        # for a NOC dashboard.
        "radar_overlay_url":      radar_overlay_url or "",
        "satellite_overlay_url":  satellite_overlay_url or "",
    })

    # Status legend HTML - pre-rendered so we don't need to template inside JS.
    legend_rows = "".join(
        f'<div class="lg-row"><span class="lg-dot" '
        f'style="background:{STATUS_COLORS[s]}"></span>{s}</div>'
        for s in ["MISSING", "FLAGGED", "INTERMITTENT",
                  "RECOVERED", "CLEAN", "NO DATA"]
    )

    # News ticker — pre-render each headline as a clickable span with
    # source badge. The whole strip auto-scrolls right-to-left via CSS
    # keyframes (no JS loop; the browser handles it).
    news_strip = ""
    if news_items:
        # Duplicate the item list so the marquee scrolls seamlessly.
        # Cap at 20 to keep HTML size reasonable.
        tops = news_items[:20]

        def _badge_color(sev: str) -> str:
            s = (sev or "").lower()
            if s in ("critical", "failure", "severe"):
                return "#dc2626"
            if s in ("warning", "alert"):
                return "#f59e0b"
            return "#38bdf8"

        def _esc(s: str) -> str:
            return (str(s or "")
                    .replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace('"', "&quot;"))

        def _item_html(it: dict) -> str:
            src = _esc(it.get("source", ""))
            title = _esc(it.get("title", ""))[:160]
            link = _esc(it.get("link", "#"))
            color = _badge_color(it.get("severity"))
            return (
                f'<a class="tk-item" href="{link}" target="_blank" rel="noopener">'
                f'<span class="tk-badge" style="color:{color};border-color:{color}">'
                f'{src}</span>{title}</a>'
            )

        strip_html = "".join(_item_html(i) for i in tops)
        # Duplicated for seamless loop.
        news_strip = (
            '<div class="ovl ticker" id="ticker">'
            '<div class="tk-inner">'
            f'{strip_html}{strip_html}'
            '</div></div>'
        )

    return _GLOBE_HTML_TEMPLATE.format(
        height_px=int(height_px),
        bg_color=bg_color,
        points_json=points_json,
        config_json=config_json,
        legend_rows=legend_rows,
        n_points=len(points),
        news_strip=news_strip,
    )


# ---------------------------------------------------------------------------
# HTML template — kept as one big string to keep this file self-contained.
# ---------------------------------------------------------------------------
_GLOBE_HTML_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>O.W.L. Globe</title>
<style>
  html, body {{ margin: 0; padding: 0; background: {bg_color};
                color: #e2e8f0; font-family: 'Inter', system-ui, sans-serif;
                overflow: hidden; height: {height_px}px; }}
  #globeViz {{ width: 100%; height: 100%; cursor: grab; }}
  #globeViz:active {{ cursor: grabbing; }}

  /* Mobile: shrink globe, hide non-essential chrome, smaller fonts */
  @media (max-width: 760px) {{
    html, body {{ height: 420px; }}
    .hud-card {{ padding: 6px 10px; }}
    .hud-clock {{ font-size: 14px; }}
    .hud-sub, .stat-card, .legend {{ font-size: 10px; }}
    .legend {{ min-width: 104px; padding: 6px 10px; }}
    .controls .ctl {{ font-size: 10px; padding: 4px 8px; }}
    .tk-item {{ font-size: 11px; }}
  }}

  .ovl {{ position: absolute; pointer-events: none; user-select: none;
          font-feature-settings: 'tnum', 'cv11'; }}

  /* Top-left HUD card */
  .hud-card {{ top: 14px; left: 14px;
               background: rgba(2, 6, 23, 0.72);
               border: 1px solid rgba(56, 189, 248, 0.25);
               border-radius: 8px; padding: 10px 14px;
               backdrop-filter: blur(6px);
               box-shadow: 0 4px 24px rgba(0,0,0,0.4); }}
  .hud-title {{ font-size: 11px; letter-spacing: 0.16em;
                color: #38bdf8; text-transform: uppercase;
                font-weight: 600; margin-bottom: 4px; }}
  .hud-clock {{ font-family: 'JetBrains Mono', ui-monospace, monospace;
                font-size: 18px; color: #f1f5f9; font-weight: 500; }}
  .hud-sub   {{ font-size: 11px; color: #64748b; margin-top: 4px; }}

  /* Bottom-left point counter — raised above the ticker */
  .stat-card {{ bottom: 44px; left: 14px;
                background: rgba(2, 6, 23, 0.72);
                border: 1px solid rgba(56, 189, 248, 0.25);
                border-radius: 8px; padding: 8px 14px; font-size: 12px;
                backdrop-filter: blur(6px); }}
  .stat-card b {{ color: #f1f5f9; }}

  /* Bottom-right legend — raised above the ticker */
  .legend {{ bottom: 44px; right: 14px;
             background: rgba(2, 6, 23, 0.78);
             border: 1px solid rgba(56, 189, 248, 0.25);
             border-radius: 8px; padding: 10px 14px;
             font-size: 11px; backdrop-filter: blur(6px);
             min-width: 130px; pointer-events: auto; }}
  .legend .title {{ color: #38bdf8; font-weight: 600;
                    text-transform: uppercase; letter-spacing: 0.1em;
                    font-size: 10px; margin-bottom: 6px; }}
  .lg-row {{ display: flex; align-items: center; gap: 8px;
             padding: 2px 0; color: #cbd5e1; }}
  .lg-dot {{ width: 9px; height: 9px; border-radius: 50%;
             box-shadow: 0 0 4px currentColor; }}

  /* Click tooltip */
  .tip {{ position: absolute; pointer-events: none;
          background: rgba(2, 6, 23, 0.92);
          border: 1px solid rgba(56, 189, 248, 0.35);
          border-radius: 6px; padding: 6px 10px;
          font-size: 12px; color: #f1f5f9;
          transform: translate(-50%, -120%);
          white-space: nowrap; display: none;
          box-shadow: 0 4px 18px rgba(0,0,0,0.6); }}
  .tip .name {{ color: #38bdf8; font-weight: 600; }}
  .tip .meta {{ color: #94a3b8; font-size: 10px; }}

  /* News ticker — auto-scrolling marquee along the bottom edge */
  .ticker {{ left: 0; right: 0; bottom: 0; height: 30px;
             background: linear-gradient(90deg,
               rgba(2,6,23,0.92) 0%,
               rgba(2,6,23,0.82) 50%,
               rgba(2,6,23,0.92) 100%);
             border-top: 1px solid rgba(56, 189, 248, 0.25);
             overflow: hidden;
             backdrop-filter: blur(4px);
             pointer-events: auto; }}
  /* The inner strip contains the item list duplicated exactly once.
     Animating from 0 to -50% scrolls through the first copy while the
     second copy slides into view — when -50% is reached, the wrapping
     to 0 is visually seamless because the first and second copies are
     identical at that boundary. No `padding-left: 100%` head-spacer,
     which used to cause a visible "jump/restart" when the animation
     wrapped. */
  .tk-inner {{ display: flex; align-items: center; height: 100%;
               white-space: nowrap; gap: 26px;
               animation: tk-scroll 35s linear infinite;
               will-change: transform; }}
  .ticker:hover .tk-inner {{ animation-play-state: paused; }}
  @keyframes tk-scroll {{
    0%   {{ transform: translate3d(0, 0, 0); }}
    100% {{ transform: translate3d(-50%, 0, 0); }}
  }}
  .tk-item {{ color: #cbd5e1; font-size: 12px; text-decoration: none;
              flex-shrink: 0; letter-spacing: 0.01em; }}
  .tk-item:hover {{ color: #f1f5f9; }}
  .tk-badge {{ display: inline-block; border: 1px solid;
               border-radius: 3px; padding: 1px 6px;
               font-size: 10px; font-weight: 600;
               letter-spacing: 0.08em; text-transform: uppercase;
               margin-right: 8px; vertical-align: 1px; }}

  /* Controls */
  .controls {{ position: absolute; top: 14px; right: 14px;
               display: flex; gap: 6px; pointer-events: auto; }}
  .ctl {{ background: rgba(2, 6, 23, 0.72);
          border: 1px solid rgba(56, 189, 248, 0.25);
          color: #cbd5e1; font-size: 11px; font-weight: 500;
          padding: 6px 10px; border-radius: 6px; cursor: pointer;
          backdrop-filter: blur(6px);
          transition: background 0.15s, color 0.15s; }}
  .ctl:hover {{ background: rgba(56, 189, 248, 0.16); color: #f1f5f9; }}
  .ctl.on   {{ background: rgba(56, 189, 248, 0.22);
               border-color: #38bdf8; color: #f1f5f9; }}

  /* Regional preset buttons — left side, vertical stack */
  .regions {{ position: absolute; top: 90px; left: 14px;
              display: flex; flex-direction: column; gap: 4px;
              pointer-events: auto; }}
  .rg-title {{ color: #38bdf8; font-weight: 600;
               text-transform: uppercase; letter-spacing: 0.1em;
               font-size: 10px; margin-bottom: 2px; }}
  .rg-btn {{ text-align: left; min-width: 110px;
             font-size: 10.5px; padding: 5px 10px; }}
  .rg-btn.active {{ background: rgba(56, 189, 248, 0.28);
                    border-color: #38bdf8; color: #f1f5f9; }}

  /* Persistent station card — shown on click (vs transient hover tooltip) */
  .card {{ position: absolute; top: 90px; right: 14px;
           width: 300px; max-width: 90vw;
           background: rgba(2, 6, 23, 0.92);
           border: 1px solid rgba(56, 189, 248, 0.35);
           border-radius: 8px; padding: 14px 16px;
           backdrop-filter: blur(6px); pointer-events: auto;
           display: none;
           box-shadow: 0 8px 28px rgba(0,0,0,0.6); }}
  .card.open {{ display: block; }}
  .card .close {{ position: absolute; top: 6px; right: 8px;
                  color: #64748b; cursor: pointer; font-size: 16px;
                  line-height: 1; padding: 2px 4px; }}
  .card .close:hover {{ color: #f1f5f9; }}
  .card .hd {{ display: flex; align-items: baseline; gap: 8px;
               margin-bottom: 8px; }}
  .card .cd-id {{ font-family: 'JetBrains Mono', ui-monospace, monospace;
                  font-size: 20px; font-weight: 700; color: #38bdf8; }}
  .card .cd-badge {{ padding: 2px 8px; border-radius: 3px;
                     font-size: 10px; font-weight: 700;
                     letter-spacing: 0.08em; text-transform: uppercase; }}
  .card .cd-sub {{ font-size: 12px; color: #94a3b8;
                   margin-bottom: 10px; }}
  .card .cd-reason {{ font-size: 11px; color: #cbd5e1;
                      padding: 6px 8px; background: rgba(56,189,248,0.08);
                      border-left: 2px solid #38bdf8;
                      border-radius: 2px; margin-bottom: 10px; }}
  .card .cd-metar {{ font-family: 'JetBrains Mono', ui-monospace, monospace;
                     font-size: 10.5px; line-height: 1.45;
                     color: #e2e8f0;
                     background: rgba(15, 23, 42, 0.72);
                     padding: 8px 10px; border-radius: 4px;
                     word-break: break-all; }}
</style>
</head>
<body>

<div id="globeViz"></div>

<div class="ovl hud-card">
  <div class="hud-title">O.W.L. NETWORK GLOBE</div>
  <div class="hud-clock" id="clock">--:--:-- UTC</div>
  <div class="hud-sub" id="sub">{n_points} stations on globe</div>
</div>

<div class="ovl stat-card">
  <b id="visible-count">{n_points}</b> visible &middot;
  <span id="hover-info">hover a point</span>
</div>

<div class="ovl controls">
  <button class="ctl on" id="rotate-btn">AUTO-ROTATE</button>
  <button class="ctl"    id="reset-btn">RESET VIEW</button>
  <button class="ctl"    id="layer-radar" title="Live NEXRAD radar composite (IEM n0q, 5 min)">RADAR</button>
  <button class="ctl"    id="layer-sat"   title="GOES-19 GeoColor satellite (NESDIS, 5 min)">SATELLITE</button>
</div>

<!-- Regional preset buttons — "too many circles to go through" mitigation -->
<div class="ovl regions">
  <div class="rg-title">REGION</div>
  <button class="ctl rg-btn" data-lat="38"    data-lng="-97"   data-alt="2.3">CONUS</button>
  <button class="ctl rg-btn" data-lat="42"    data-lng="-72"   data-alt="1.1">NORTHEAST</button>
  <button class="ctl rg-btn" data-lat="32"    data-lng="-84"   data-alt="1.1">SOUTHEAST</button>
  <button class="ctl rg-btn" data-lat="41"    data-lng="-93"   data-alt="1.1">CENTRAL</button>
  <button class="ctl rg-btn" data-lat="38"    data-lng="-110"  data-alt="1.1">WEST</button>
  <button class="ctl rg-btn" data-lat="64"    data-lng="-150"  data-alt="1.0">ALASKA</button>
  <button class="ctl rg-btn" data-lat="20.7"  data-lng="-157"  data-alt="0.7">HAWAII</button>
  <button class="ctl rg-btn" data-lat="18"    data-lng="-66"   data-alt="0.7">CARIBBEAN</button>
</div>

<div class="ovl legend">
  <div class="title">Status</div>
  {legend_rows}
</div>

<div class="tip" id="tip"></div>

<!-- Persistent station card shown on click -->
<div class="card ovl" id="card">
  <span class="close" id="card-close">X</span>
  <div class="hd">
    <span class="cd-id"    id="cd-id">----</span>
    <span class="cd-badge" id="cd-badge">---</span>
  </div>
  <div class="cd-sub"    id="cd-sub"></div>
  <div class="cd-reason" id="cd-reason" style="display:none;"></div>
  <div class="cd-metar"  id="cd-metar"></div>
</div>

{news_strip}

<!-- three.js + globe.gl from CDN. Pinned versions for cache stability. -->
<script src="https://unpkg.com/three@0.160.0/build/three.min.js"></script>
<script src="https://unpkg.com/globe.gl@2.32.0/dist/globe.gl.min.js"></script>

<script>
(() => {{
  const POINTS = {points_json};
  const CFG    = {config_json};

  // ----- Globe init ------------------------------------------------------
  const world = Globe()
    (document.getElementById('globeViz'))
    .backgroundColor(CFG.bg_color)
    .globeImageUrl(CFG.earth_texture)
    .bumpImageUrl(CFG.bump_texture)
    .showAtmosphere(CFG.atmosphere_alt > 0)
    .atmosphereColor(CFG.atmosphere_color)
    .atmosphereAltitude(CFG.atmosphere_alt)
    .pointsData(POINTS)
    .pointLat('lat')
    .pointLng('lon')
    .pointAltitude('alt')
    .pointRadius('radius')
    .pointColor('color')
    .pointResolution(8)
    .pointsMerge(false)        // each point clickable separately
    .pointsTransitionDuration(700);

  if (CFG.starfield) {{
    world.backgroundImageUrl(CFG.night_texture);
  }}

  // Auto-rotate slowly until the user grabs.
  if (CFG.auto_rotate) {{
    const ctrls = world.controls();
    ctrls.autoRotate = true;
    ctrls.autoRotateSpeed = 0.45;
  }}

  // Default camera altitude: pulled back enough to see all of N. America.
  world.pointOfView({{ lat: 38, lng: -97, altitude: 2.3 }}, 0);

  // ----- Tooltip + click -------------------------------------------------
  const tip = document.getElementById('tip');
  const hoverInfo = document.getElementById('hover-info');

  // ----- Persistent click card ------------------------------------------
  const card        = document.getElementById('card');
  const cdId        = document.getElementById('cd-id');
  const cdBadge     = document.getElementById('cd-badge');
  const cdSub       = document.getElementById('cd-sub');
  const cdReason    = document.getElementById('cd-reason');
  const cdMetar     = document.getElementById('cd-metar');
  document.getElementById('card-close').addEventListener('click', () => {{
    card.classList.remove('open');
  }});

  world
    .onPointHover(p => {{
      if (!p) {{
        tip.style.display = 'none';
        hoverInfo.textContent = 'hover or click a point';
        return;
      }}
      hoverInfo.innerHTML = `<b style="color:${{p.color}}">${{p.station}}</b> &middot; ${{p.status}}`;
      tip.innerHTML =
        `<div class="name">${{p.station}} &middot; ${{p.status}}</div>` +
        `<div>${{p.name || ''}}${{p.state ? ', ' + p.state : ''}}</div>` +
        (p.reason ? `<div class="meta">${{p.reason}}</div>` : '');
      tip.style.display = 'block';
    }})
    .onPointClick((p, ev) => {{
      if (!p) return;

      // Fill + open the persistent card.
      cdId.textContent = p.station;
      cdBadge.textContent = p.status;
      cdBadge.style.background = p.color;
      cdBadge.style.color = '#020617';
      cdSub.textContent =
        (p.name || '') + (p.state ? ', ' + p.state : '');
      if (p.reason) {{
        cdReason.textContent = p.reason;
        cdReason.style.display = 'block';
      }} else {{
        cdReason.style.display = 'none';
      }}
      cdMetar.textContent = p.latest_metar || '(no recent METAR)';
      card.classList.add('open');

      // Also inform the parent Streamlit page so the drill panel below
      // the globe can update its selectbox to match.
      try {{
        window.parent.postMessage({{
          type: 'owl.station.click',
          station: p.station, name: p.name,
          state: p.state, status: p.status,
        }}, '*');
      }} catch (e) {{ console.warn('postMessage failed', e); }}

      // Zoom the camera to the point.
      world.pointOfView({{
        lat: p.lat, lng: p.lon, altitude: 0.95,
      }}, 900);
    }});

  // ----- Radar overlay toggle — Three.js sphere texture -----------------
  // The IEM n0q CONUS composite is a 2D PNG covering lon [-126..-66],
  // lat [24..50].  To wrap it onto the 3D globe we draw it into an
  // equirectangular 2048x1024 canvas at the right UV coordinates, then
  // apply that canvas as a CanvasTexture on a sphere mesh slightly
  // larger than the base globe.  The base globe stays visible everywhere
  // the canvas is transparent (which is most of it — CONUS only).
  //
  // Image must be served with CORS.  IEM's static PNG URLs send
  // `Access-Control-Allow-Origin: *` so `crossOrigin='anonymous'` works.

  const GLOBE_RADIUS = 100;        // globe.gl default
  const OVERLAY_RADIUS = 100.3;    // just above surface to avoid z-fighting

  // Bounds for each overlay (lon_min, lat_min, lon_max, lat_max).
  const RADAR_BBOX = [-126, 24, -66, 50];
  const GOES_BBOX  = [-135, 15, -55, 55];   // approx GOES CONUS coverage

  function buildEquirectTexture(imgUrl, bbox) {{
    return new Promise((resolve, reject) => {{
      const img = new Image();
      img.crossOrigin = 'anonymous';
      img.onerror = (e) => reject(e);
      img.onload = () => {{
        const W = 2048, H = 1024;
        const canvas = document.createElement('canvas');
        canvas.width = W; canvas.height = H;
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, W, H);
        const [lonMin, latMin, lonMax, latMax] = bbox;
        const x = ((lonMin + 180) / 360) * W;
        const w = ((lonMax - lonMin) / 360) * W;
        const y = ((90 - latMax) / 180) * H;
        const h = ((latMax - latMin) / 180) * H;
        ctx.drawImage(img, x, y, w, h);
        const tex = new THREE.CanvasTexture(canvas);
        tex.needsUpdate = true;
        resolve(tex);
      }};
      img.src = imgUrl;
    }});
  }}

  function makeOverlayMesh(texture, opacity = 0.7, blending = THREE.NormalBlending) {{
    const geom = new THREE.SphereGeometry(OVERLAY_RADIUS, 90, 60);
    const mat = new THREE.MeshBasicMaterial({{
      map: texture,
      transparent: true,
      opacity: opacity,
      blending: blending,
      depthWrite: false,
      side: THREE.DoubleSide,
    }});
    return new THREE.Mesh(geom, mat);
  }}

  // Track meshes so we can add/remove.
  let radarMesh = null;
  let satMesh   = null;

  async function toggleOverlay(kind) {{
    const isRadar = kind === 'radar';
    const btn = isRadar ? document.getElementById('layer-radar')
                        : document.getElementById('layer-sat');
    const want = !btn.classList.contains('on');
    btn.classList.toggle('on', want);

    const scene = world.scene();
    let mesh = isRadar ? radarMesh : satMesh;

    if (!want) {{
      if (mesh) {{
        scene.remove(mesh);
        mesh.material.map.dispose();
        mesh.material.dispose();
        mesh.geometry.dispose();
        if (isRadar) radarMesh = null; else satMesh = null;
      }}
      return;
    }}

    // Build the overlay mesh.
    btn.textContent = isRadar ? 'RADAR (loading...)' : 'SATELLITE (loading...)';
    const url = isRadar ? CFG.radar_overlay_url : CFG.satellite_overlay_url;
    const bbox = isRadar ? RADAR_BBOX : GOES_BBOX;
    try {{
      const tex = await buildEquirectTexture(url, bbox);
      const opac = isRadar ? 0.78 : 0.55;
      const blend = isRadar ? THREE.AdditiveBlending : THREE.NormalBlending;
      mesh = makeOverlayMesh(tex, opac, blend);
      scene.add(mesh);
      if (isRadar) radarMesh = mesh; else satMesh = mesh;
      btn.textContent = isRadar ? 'RADAR' : 'SATELLITE';
    }} catch (e) {{
      console.warn('overlay load failed', e);
      btn.textContent = isRadar ? 'RADAR (failed)' : 'SATELLITE (failed)';
      btn.classList.remove('on');
    }}
  }}

  const radarBtn = document.getElementById('layer-radar');
  const satBtn   = document.getElementById('layer-sat');
  if (radarBtn && CFG.radar_overlay_url) {{
    radarBtn.addEventListener('click', () => toggleOverlay('radar'));
  }} else if (radarBtn) {{
    radarBtn.textContent = 'RADAR (unavailable)';
    radarBtn.disabled = true;
  }}
  if (satBtn && CFG.satellite_overlay_url) {{
    satBtn.addEventListener('click', () => toggleOverlay('sat'));
  }} else if (satBtn) {{
    satBtn.textContent = 'SATELLITE (unavailable)';
    satBtn.disabled = true;
  }}

  // ----- Region preset buttons -----------------------------------------
  document.querySelectorAll('.rg-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const lat = parseFloat(btn.dataset.lat);
      const lng = parseFloat(btn.dataset.lng);
      const alt = parseFloat(btn.dataset.alt);
      world.pointOfView({{ lat, lng, altitude: alt }}, 900);
      document.querySelectorAll('.rg-btn.active').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      // Stop auto-rotate when user picks a region.
      const ctrls = world.controls();
      if (ctrls.autoRotate) {{
        ctrls.autoRotate = false;
        rotateBtn.classList.remove('on');
      }}
    }});
  }});

  // Track mouse for tooltip positioning.
  document.getElementById('globeViz').addEventListener('mousemove', e => {{
    if (tip.style.display === 'block') {{
      tip.style.left = e.clientX + 'px';
      tip.style.top  = e.clientY + 'px';
    }}
  }});

  // ----- Controls --------------------------------------------------------
  const rotateBtn = document.getElementById('rotate-btn');
  rotateBtn.addEventListener('click', () => {{
    const ctrls = world.controls();
    ctrls.autoRotate = !ctrls.autoRotate;
    rotateBtn.classList.toggle('on', ctrls.autoRotate);
  }});

  document.getElementById('reset-btn').addEventListener('click', () => {{
    world.pointOfView({{ lat: 38, lng: -97, altitude: 2.3 }}, 900);
  }});

  // Stop auto-rotate as soon as the user grabs the globe.
  document.getElementById('globeViz').addEventListener('pointerdown', () => {{
    const ctrls = world.controls();
    if (ctrls.autoRotate) {{
      ctrls.autoRotate = false;
      rotateBtn.classList.remove('on');
    }}
  }});

  // ----- Live clock ------------------------------------------------------
  const clock = document.getElementById('clock');
  function tick() {{
    const d = new Date();
    const t = d.toISOString().slice(11, 19);
    clock.textContent = t + ' UTC';
  }}
  tick();
  setInterval(tick, 1000);

  // ----- Resize ----------------------------------------------------------
  window.addEventListener('resize', () => {{
    world.width(window.innerWidth).height(window.innerHeight);
  }});
}})();
</script>
</body>
</html>
"""
