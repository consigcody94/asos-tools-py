#!/bin/sh
# O.W.L. — container entrypoint for HF Spaces Docker.
#
# Prepares writable runtime paths, then hands control to supervisord which
# launches nginx + streamlit + uvicorn under one PID.

set -eu

# ---- nginx needs these temp dirs under /tmp so a non-root user can write.
mkdir -p /tmp/nginx/body /tmp/nginx/proxy /tmp/nginx/fastcgi \
         /tmp/nginx/uwsgi /tmp/nginx/scgi

# ---- DiskCache: prefer HF Spaces persistent storage if available.
# HF mounts the persistent volume at /data when the $5/mo add-on is enabled.
# Otherwise fall back to /tmp (ephemeral, reset on every container rebuild).
if [ -d /data ] && [ -w /data ]; then
    export OWL_CACHE_DIR="${OWL_CACHE_DIR:-/data/cache}"
else
    export OWL_CACHE_DIR="${OWL_CACHE_DIR:-/tmp/owl-cache}"
fi
mkdir -p "$OWL_CACHE_DIR"

# ---- Streamlit expects $HOME to be writable for its config dir.
export HOME="${HOME:-/tmp}"
mkdir -p "$HOME/.streamlit"

echo "[entrypoint] OWL_CACHE_DIR=$OWL_CACHE_DIR"
echo "[entrypoint] HOME=$HOME"
echo "[entrypoint] launching supervisord"

exec /usr/bin/supervisord -c /app/deploy/supervisord.conf
