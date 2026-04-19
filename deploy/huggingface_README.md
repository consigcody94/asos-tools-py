---
title: O.W.L. Observation Watch Log
emoji: 🌤️
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 7860
pinned: true
license: mit
short_description: Federal-grade ASOS network observation & maintenance monitor
---

# O.W.L. — Observation Watch Log

Federal-grade monitoring for the ASOS (Automated Surface Observing System)
network — 920 NOAA / NWS / FAA / DOD stations, live maintenance-flag
detection, missing-report surveillance, METAR/TAF/SIGMET/AIRMET/PIREP
overlays, 926 FAA WeatherCams, NWS CAP alerts, scheduled scans, and a
3D satellite globe as the primary view.

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

### Data sources

| Source | Used for | Auth | Trust |
|---|---|---|---|
| IEM (Iowa Environmental Mesonet) | METAR + 1-min archive | none | mirror |
| NCEI Access Services | IEM fallback | none | federal |
| NWS api.weather.gov | Current conditions + CAP alerts | none (UA required) | federal |
| AWC Aviation Weather Center | METAR/TAF/SIGMET/AIRMET/PIREP | none | federal |
| FAA WeatherCams | 926 live airport cameras | browser headers | federal |
| NOAA / FAA / NTSB / AWC RSS | News ticker | none | federal |
| NCEI HOMR | 920-station AOMC catalog | none | federal |

---

## Space configuration (set these under Settings -> Variables and secrets)

| Name               | Kind   | Required | Purpose                                        |
|--------------------|--------|----------|------------------------------------------------|
| `OWL_CRON_SECRET`  | secret | yes      | Shared secret for `/api/tick`; must match the  GitHub repo secret that fires the cron |
| `OWL_ALERT_URLS`   | secret | optional | Comma-separated Apprise URLs for Slack/Discord/Teams/PagerDuty notifications |
| `OWL_SCAN_HOURS`   | var    | optional | Watchlist look-back window (default `4`)       |
| `OWL_LOG_LEVEL`    | var    | optional | Python log level (default `INFO`)              |
| `FAA_NOTAM_KEY`    | secret | optional | FAA NOTAM API key for planned-outage overlay   |
| `IEM_API_BASE`     | var    | optional | Override IEM endpoint (testing only)           |
| `NCEI_API_BASE`    | var    | optional | Override NCEI endpoint (testing only)          |
| `OWL_CACHE_DIR`    | var    | optional | DiskCache path (auto-picks `/data` if persistent storage add-on is enabled) |

### Persistent storage (recommended, $5/mo)

Enable **Persistent Storage** in Space settings. The container detects `/data`
and DiskCache writes there — 1-minute fetches, watchlist scans, and webcam
thumbnails survive rebuilds. Without it, the cache is ephemeral under
`/tmp/owl-cache` (works fine, just cold after restarts).

---

## Links

- Source: https://github.com/consigcody94/asos-tools-py
- Health: https://consgicody-asos-tools.hf.space/api/health
- Sources registry: https://consgicody-asos-tools.hf.space/api/sources
