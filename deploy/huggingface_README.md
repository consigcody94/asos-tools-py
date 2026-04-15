---
title: ASOS Tools
emoji: 🌤️
colorFrom: blue
colorTo: purple
sdk: streamlit
sdk_version: "1.39.0"
app_file: app.py
pinned: true
license: mit
short_description: Pull NCEI 1-minute ASOS surface observations by date range
---

# ASOS Tools · 1-minute surface observations

Interactive dashboard for NOAA/NCEI ASOS weather station data — temperature,
wind, pressure, precipitation, visibility — at **1-minute resolution**, plus
a dedicated view of ASOS maintenance-flag (`$`) reporting.

Powered by:
- [`asos-tools-py`](https://github.com/consigcody94/asos-tools-py) Python package
- Data from the [Iowa Environmental Mesonet (IEM) ASOS service](https://mesonet.agron.iastate.edu/), which ingests the [NOAA/NCEI 1-minute archive](https://www.ncei.noaa.gov/data/automated-surface-observing-system-one-minute-pg1/)

## How to deploy this as your own Space

1. Create a new Space at https://huggingface.co/new-space
2. SDK: **Streamlit**
3. Clone the Space repo locally, then copy these files into it:
   - `app.py` (Streamlit entrypoint)
   - `requirements.txt`
   - `asos_tools/` (Python package, *including* `stations.py`, `metars.py`, `fetch.py`, `report.py`, `__init__.py`)
   - This `README.md` (with the YAML frontmatter above)
4. `git push` — HF will build and serve automatically (~2 min first build).

All data is fetched live on each query; nothing is stored.
