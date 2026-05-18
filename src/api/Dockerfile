# ── TaaSim API — FastAPI service ────────────────────────────────────────────
# python:3.11-slim keeps the image small while providing a modern Python.
# Build context: src/api/
# ---------------------------------------------------------------------------
FROM python:3.11-slim

# System deps: curl is used only by the Docker healthcheck. libev4 provides
# runtime support for the cassandra-driver's libev event loop (extremely lightweight, ~20KB).
# We avoid installing heavy compiler tools (gcc, libev-dev) since pip downloads precompiled binary wheels!
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    sed -i 's/deb.debian.org/cdn-fastly.deb.debian.org/g' /etc/apt/sources.list || true \
    && sed -i 's/deb.debian.org/cdn-fastly.deb.debian.org/g' /etc/apt/sources.list.d/debian.sources || true \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        libev4

WORKDIR /app

# Install Python dependencies first (using BuildKit pip cache to keep downloaded wheels on the host
# and a large timeout to prevent timeouts on slow connections).
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=1000 -r requirements.txt

# Copy application source.
COPY main.py .

EXPOSE 8000

# Use 1 worker in dev (matches single-node setup).  Increase workers in prod.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
