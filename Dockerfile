# syntax=docker/dockerfile:1

# Two images are built from this file (CI builds both, tags `:<env>` and
# `:<env>-cp`):
#   app  — the full monorepo image every code-location gRPC server runs
#          (riot_api, leaguepedia, chatbot; different `-m orchestration.<loc>`
#          entrypoints). Carries the heavy ML stack because the chatbot location
#          needs torch/whisperx.
#   cp   — control plane (webserver + daemon). Deliberately code-free, so code
#          pushes never change its digest and the UI/daemon stop cycling on every
#          deploy. Just dagster core + the Postgres storage driver.

# ── base ─────────────────────────────────────────────────────────────────────
# Shared foundation: uv + dependency manifests only (no code),
# so both images build off the same cached layers.
FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /opt/dagster/app
COPY pyproject.toml uv.lock ./

# ── app (user-code image) ─────────────────────────────────────────────────────
FROM base AS app
ARG IMAGE_SOURCE
# System deps.
# - ffmpeg: required by the FFmpegVideoConvertor postprocessor,
#   and by whisperx to decode audio;
# - deno: the JavaScript runtime yt-dlp uses for YouTube extraction (nsig / PO tokens).
# Installed before the code copy so it caches independently of source changes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno
# Copy workspace packages and the root source code
COPY packages/ ./packages/
COPY src/ ./src/
# Instance + workspace config (DAGSTER_HOME points here at runtime).
COPY dagster.yaml workspace.yaml ./
# Install the packages as standard, immutable wheels.
# --frozen respects uv.lock;
# --no-cache keeps the image lean;
# --no-editable guarantees no symlinks (a true production artifact);
# --group server pulls in dagster-webserver + dagster-postgres so the one image can run every OSS role.
RUN uv sync --no-dev --extra private --group server --frozen --no-cache --no-editable
# Freeze the locked dependency versions for the cp image to pin against, so the
# webserver/daemon stay version-compatible with the code-location gRPC servers.
# --no-emit-workspace drops the local path entries (invalid as constraints in the
# code-free cp stage); --no-hashes avoids hash-pinning the cp install.
RUN uv export --frozen --no-dev --group server --no-hashes --no-emit-workspace \
    --format requirements-txt -o /tmp/cp-constraints.txt

# Add the venv's bin to PATH so dagster is found
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src
# DAGSTER_HOME = dir holding dagster.yaml (instance config) for every OSS role.
ENV DAGSTER_HOME=/opt/dagster/app

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}

# ── cp (control-plane image) ───────────────────────────────────────────────────
# No packages/, no src/ — only config + dagster core.
FROM base AS cp
ARG IMAGE_SOURCE
COPY --from=app /tmp/cp-constraints.txt ./cp-constraints.txt
COPY dagster.yaml workspace.yaml ./
RUN uv venv .venv \
    && VIRTUAL_ENV=/opt/dagster/app/.venv uv pip install --no-cache \
    -c cp-constraints.txt dagster dagster-webserver dagster-postgres

ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
ENV DAGSTER_HOME=/opt/dagster/app

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}
