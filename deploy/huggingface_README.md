---
title: O.W.L. Observation Watch Log
emoji: 🌤️
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 7860
pinned: true
license: mit
short_description: ASOS network observation & maintenance monitor
---

# O.W.L. — Observation Watch Log

**Author:** Cody Churchwell &mdash; CTO, Sentinel OWL &mdash; [`cto@sentinelowl.org`](mailto:cto@sentinelowl.org)
**Source:** <https://github.com/consigcody94/asos-tools-py>

AOMC-grade monitoring for the ASOS (Automated Surface Observing System)
network — 920 NOAA / NWS / FAA / DOD stations. Live maintenance-flag
detection, missing-report surveillance, per-station live radar + satellite
+ webcam loops, tropical / earthquake / buoy / NOTAM site correlation,
NWS forecaster tooling (SIGMET / AIRMET / PIREP / TAF / AFD / CAP alerts),
scheduled network scans, and a 3D satellite globe as the primary view.

Thirteen authoritative public feeds. Zero commercial sources. Zero scraped
sources. Everything surfaced in the UI — nothing on a shelf.

---

## Architecture

One Docker container, three processes under `supervisord`, fronted by an
internal `nginx` reverse proxy:

```
Internet
   |
   v   (HTTPS, HF edge)
[:7860] nginx  ┬── /api/*  -> uvicorn   (FastAPI, :8000)
               └── /*      -> streamlit (UI + WebSockets, :8501)
```

External scheduler: a GitHub Actions workflow (`.github/workflows/owl-tick.yml`)
POSTs `/api/tick` every 5 minutes. The FastAPI handler returns `202` immediately
and runs the scan as a `BackgroundTask`, so the scheduler survives container
restarts without any in-process APScheduler state.

---

## Data sources (13 feeds, all public, all surfaced in the UI)

| Source | UI surface | Auth |
|---|---|---|
| IEM (Iowa Environmental Mesonet) | Primary METAR + 1-min archive | none |
| NCEI Access Services | IEM fallback + authoritative archive | none |
| NWS `api.weather.gov` | Current conditions + CAP alerts | UA required |
| AWC Aviation Weather Center | METAR / TAF / SIGMET / AIRMET / PIREP / **AFD** | none |
| NWS RIDGE NEXRAD | Per-station WSR-88D animated radar loops (159 sites) | none |
| NESDIS GOES-19 East | Satellite loops: CONUS + NE / SE / UMV / SMV / NR / SR / PR | none |
| NESDIS GOES-18 West | Satellite loops: AK / HI / PNW / PSW (Pacific coverage) | none |
| USGS Earthquake Hazards | Per-site proximity (M2.5+ within 300 km) + national 7-day significant | none |
| NOAA NHC `CurrentStorms.json` | Active tropical cyclones + 500 km station proximity | none |
| NOAA NDBC buoys | Coastal cross-check (402 met-enabled stations) | none |
| FAA WeatherCams | Nearest-cam animated 5-frame loops | browser headers |
| NOAA SWPC | Kp index + X-ray flux + geomagnetic alerts | none |
| FAA NOTAM API | Per-station planned-outage correlation (optional, key-gated) | client_id + secret |

### Drill panel (per-station view)

Pick any station → METAR + webcam grid → **Live Coverage** (FAA cam ·
NEXRAD · GOES satellite side-by-side) → **Site Hazards** (recent
earthquakes · active tropical systems · nearest NDBC buoy · FAA NOTAMs).

### Forecasters tab

Six sub-tabs: Active Hazards (SIGMETs + AIRMETs + PIREPs + **tropical
systems**) → Station TAF / METAR → Flight Category Rollup → Active
Alerts (CAP + **PAGER-significant earthquakes**) → **Regional
Discussion (AFD)** → Space Weather.

### AOMC Controllers tab

Network watchlist (920 stations), flagged / missing / intermittent
rollups, per-station drill, missing-report + incident DOCX export,
3D globe with radar + satellite overlay toggles.

---

## Space configuration (set these under Settings → Variables and secrets)

| Name                       | Kind   | Required | Purpose                                        |
|----------------------------|--------|----------|------------------------------------------------|
| `OWL_CRON_SECRET`          | secret | yes      | Shared secret for `/api/tick`; must match the GitHub repo secret that fires the cron |
| `OWL_ALERT_URLS`           | secret | optional | Comma-separated Apprise URLs for Slack / Discord / Teams / PagerDuty notifications |
| `OWL_SCAN_HOURS`           | var    | optional | Watchlist look-back window (default `4`)       |
| `OWL_LOG_LEVEL`            | var    | optional | Python log level (default `INFO`)              |
| `FAA_NOTAM_CLIENT_ID`      | secret | optional | FAA NOTAM API client ID — enables per-station NOTAM correlation in Site Hazards |
| `FAA_NOTAM_CLIENT_SECRET`  | secret | optional | FAA NOTAM API client secret (paired with above) |
| `IEM_API_BASE`             | var    | optional | Override IEM endpoint (testing only)           |
| `NCEI_API_BASE`            | var    | optional | Override NCEI endpoint (testing only)          |
| `OWL_CACHE_DIR`            | var    | optional | DiskCache path (auto-picks `/data` if persistent storage add-on is enabled) |

### Persistent storage (recommended, $5/mo)

Enable **Persistent Storage** in Space settings. The container detects `/data`
and DiskCache writes there — 1-minute fetches, watchlist scans, webcam
thumbnails, radar / satellite GIF metadata, quake / tropical caches survive
rebuilds. Without it, the cache is ephemeral under `/tmp/owl-cache` (works
fine, just cold after restarts).

---

## Links

- Source: <https://github.com/consigcody94/asos-tools-py>
- Health: <https://consgicody-asos-tools.hf.space/api/health>
- Sources registry: <https://consgicody-asos-tools.hf.space/api/sources>
- Author: Cody Churchwell — <cto@sentinelowl.org>
