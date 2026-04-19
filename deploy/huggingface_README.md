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
network — 920 NOAA / NWS / FAA / DOD stations, maintenance flag detection,
missing-report surveillance, live METAR/TAF/SIGMET/AIRMET/PIREP, NWS CAP
alerts, incident report generation.

## Architecture (Docker multi-process)

This Space runs **one Docker container, three processes**, coordinated by
`supervisord` and fronted by an internal `nginx` reverse proxy:

```
Internet -> HF Spaces :7860 -> nginx
                                 |-- /api/*  -> uvicorn  (FastAPI)
                                 `-- /*      -> streamlit (UI)
```

The FastAPI side-car exposes `/api/health`, `/api/tick`, `/api/sources`,
`/api/webcams/near`. `/api/tick` is the external-cron webhook that replaces
in-container APScheduler — a GitHub Actions scheduled workflow
(`.github/workflows/owl-tick.yml`, runs every 5 min) POSTs to it, so the
scheduler survives container restarts.

## Required Space configuration

Set these under **Settings → Variables and secrets** on the HF Space:

| Name                | Type   | Purpose                                                    |
|---------------------|--------|------------------------------------------------------------|
| `OWL_CRON_SECRET`   | secret | Shared secret for `/api/tick`; must match a matching secret in the GitHub repo that posts to it |
| `OWL_ALERT_URLS`    | secret | Optional. Comma-separated Apprise URLs (Slack/Discord/etc) |
| `OWL_SCAN_HOURS`    | var    | Optional, default `4`. Watchlist look-back window          |

## Optional: persistent cache

Enable **Persistent Storage** ($5/mo, 20 GB) in Space settings. The container
mounts it at `/data` and DiskCache writes to `/data/cache`, so 1-min fetches
and watchlist scans survive rebuilds. Without it, cache is ephemeral under
`/tmp/owl-cache` and resets on every container restart — still fully
functional, just cold after rebuilds.

## Links

- Source: https://github.com/consigcody94/asos-tools-py
- Data: IEM, AWC, NWS, NCEI, FAA (all federal-authoritative or federal-mirror)
- API: `https://consgicody-asos-tools.hf.space/api/health`
