# =============================================================================
# O.W.L. — Observation Watch Log
# Multi-stage container for Hugging Face Spaces (Docker SDK, port 7860).
#
# Architecture: one container, three processes under supervisord:
#   nginx     -> reverse proxy on :7860 (the only port HF exposes)
#   streamlit -> UI on 127.0.0.1:8501
#   uvicorn   -> FastAPI REST/webhook on 127.0.0.1:8000
#
# nginx routes /api/* to FastAPI and everything else (including websockets)
# to Streamlit.  An external GitHub Actions cron posts to /api/tick every
# 5 minutes, so the scheduler survives container restarts without needing
# any in-process APScheduler state.
# =============================================================================


# -----------------------------------------------------------------------------
# Stage 1 — wheels
#
# Build-only image with gcc/build-essential available.  Compiles any
# pandas/pyarrow/stumpy wheels that don't ship pre-built for linux/amd64,
# then stages them under /wheels for the runtime image to copy in without
# carrying the 200 MB build toolchain.
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS wheels

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .

RUN pip wheel --no-cache-dir --wheel-dir=/wheels -r requirements.txt


# -----------------------------------------------------------------------------
# Stage 2 — runtime
#
# Slim image with only the runtime OS packages.  Installs all Python deps
# from the /wheels stage via `pip --no-index`, so no network access is
# needed post-build and no build-essential is pulled in here.
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# ---- Runtime OS packages -----------------------------------------------------
# fonts-dejavu-core : matplotlib needs a font for report rendering
# nginx             : internal reverse proxy on :7860
# supervisor        : PID 1 supervisor for the 3 processes
# curl              : HEALTHCHECK + debug
# tini              : proper SIGTERM forwarding + zombie reaping
RUN apt-get update && apt-get install -y --no-install-recommends \
        fonts-dejavu-core \
        nginx \
        supervisor \
        curl \
        tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- Python deps from wheels (no gcc needed) --------------------------------
COPY --from=wheels /wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
 && rm -rf /wheels

# ---- App source --------------------------------------------------------------
COPY asos_tools/         ./asos_tools/
COPY app.py              .
COPY owl_logo.png        .
COPY assets/             ./assets/
COPY .streamlit/         ./.streamlit/
COPY deploy/             ./deploy/

# ---- Writable runtime paths --------------------------------------------------
# All nginx temp/log paths redirected to /tmp so we run safely as any UID.
RUN chmod +x /app/deploy/entrypoint.sh \
 && mkdir -p /tmp/nginx/body /tmp/nginx/proxy /tmp/nginx/fastcgi \
             /tmp/nginx/uwsgi /tmp/nginx/scgi \
 && mkdir -p /tmp/.streamlit /tmp/owl-cache \
 && chmod -R 0777 /tmp/nginx /tmp/.streamlit /tmp/owl-cache

# ---- Env ---------------------------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_CORS=false \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    HOME=/tmp \
    OWL_CACHE_DIR=/tmp/owl-cache \
    OWL_LOG_LEVEL=INFO

# HF exposes exactly this port externally.
EXPOSE 7860

# Liveness probe — nginx must answer, which implies streamlit/fastapi are up
# enough for the proxy to route.  HF honors HEALTHCHECK in its UI.
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://127.0.0.1:7860/api/health >/dev/null || exit 1

# tini reaps zombie children + forwards SIGTERM cleanly to supervisord.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/deploy/entrypoint.sh"]
