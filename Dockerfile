# =============================================================================
# O.W.L. — Observation Watch Log
# Multi-process container for Hugging Face Spaces (Docker SDK, port 7860).
#
# One container runs three processes under supervisord:
#   * nginx     -> reverse proxy on :7860 (the only port HF exposes)
#   * streamlit -> UI on 127.0.0.1:8501
#   * uvicorn   -> FastAPI REST/webhook on 127.0.0.1:8000
#
# nginx routes /api/* to FastAPI and everything else (including websockets)
# to Streamlit.  An external GitHub Actions cron posts to /api/tick every
# 5 minutes, so the scheduler survives container restarts without needing
# any in-process APScheduler state.
# =============================================================================

FROM python:3.12-slim

# ---- System packages ---------------------------------------------------------
# fonts-dejavu-core : matplotlib needs a font to render reports
# nginx             : internal reverse proxy
# supervisor        : PID 1 supervisor for the three processes
# curl              : healthcheck + debugging
# tini              : proper signal handling for supervisord
RUN apt-get update && apt-get install -y --no-install-recommends \
        fonts-dejavu-core \
        nginx \
        supervisor \
        curl \
        tini \
 && rm -rf /var/lib/apt/lists/*

# ---- Python deps -------------------------------------------------------------
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- App source --------------------------------------------------------------
COPY asos_tools/         ./asos_tools/
COPY app.py              .
COPY owl_logo.png        .
COPY assets/             ./assets/
COPY .streamlit/         ./.streamlit/
COPY deploy/             ./deploy/

# ---- Runtime prep ------------------------------------------------------------
# Every nginx temp path is redirected to /tmp so we can run as any UID.
# Streamlit's config dir lives under $HOME, which we set to /tmp as well.
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
    OWL_CACHE_DIR=/tmp/owl-cache

# HF exposes exactly this port externally.
EXPOSE 7860

# Liveness probe — nginx must answer, which implies streamlit/fastapi are up
# enough for the proxy to route.  HF honors HEALTHCHECK in its UI.
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://127.0.0.1:7860/api/health >/dev/null || exit 1

# tini reaps zombie children + forwards SIGTERM cleanly to supervisord.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/deploy/entrypoint.sh"]
