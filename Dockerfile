# syntax=docker/dockerfile:1.7-labs

# Three stages, ordered so the heavy dependency install caches across code changes:
#   deps — installs only third-party dependencies (torch/whisperx/CUDA, multi-GB) from
#          the dependency manifests, with NO application source.
#          This layer is therefore invalidated only by a lockfile/manifest change.
#   app  — the full monorepo image every code-location gRPC server runs
#          (different `-m orchestration.<loc>` entrypoints).
#          Adds system deps + source, then installs the workspace packages themselves -
#          a fast relink, since deps are already present.
#   cp   — control plane (webserver + daemon).
#          Deliberately code-free, so code pushes never change its digest
#          and the UI/daemon stop cycling on every deploy.
#          Just dagster core + the Postgres storage driver, pinned to app's resolved versions.

# ── base ─────────────────────────────────────────────────────────────────────
# Shared foundation: uv only.
# Both the deps/app chain and the code-free cp image
# build off this, so they share the uv layer.
FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /opt/dagster/app

# ── deps (intermediate) ──────────────────────────────────────────────────────
# Copy ONLY the dependency manifests:
# - The root pyproject/lockfile;
# - Every workspace member's pyproject (--parents preserves the <tier>/<name>/ layout).
# Then install the external dependencies WITHOUT the local packages (--no-install-workspace).
# No application source is present, so this multi-GB layer is reused on every code-only push
# and rebuilds only when a manifest or the lockfile changes.
FROM base AS deps
COPY pyproject.toml uv.lock ./
COPY --parents library/*/pyproject.toml pipelines/*/pyproject.toml ./
RUN uv sync --no-dev --extra private --group server --frozen --no-install-workspace --no-cache

# ── app (user-code image) ────────────────────────────────────────────────────
FROM deps AS app
ARG IMAGE_SOURCE
# System deps.
# - ffmpeg: required by the FFmpegVideoConvertor postprocessor, and by whisperx to decode audio;
# - deno: the JavaScript runtime yt-dlp uses for YouTube extraction (nsig / PO tokens).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno
# Copy workspace packages and the root source code.
COPY library/ ./library/
COPY pipelines/ ./pipelines/
COPY src/ ./src/
# Instance + workspace config (DAGSTER_HOME points here at runtime).
COPY dagster.yaml workspace.yaml ./
# Install the workspace packages themselves.
# External deps are already in the venv from the `deps` stage,
# so this only links the local packages and is fast.
# --no-editable guarantees no symlinks (a true production artifact).
RUN uv sync --no-dev --extra private --group server --frozen --no-cache --no-editable
# Freeze the locked dependency versions for the cp image to pin against,
# so the webserver/daemon stay version-compatible with the code-location gRPC servers.
# --no-emit-workspace drops the local path entries
# (invalid as constraints in the code-free cp stage);
# --no-hashes avoids hash-pinning the cp install.
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
# No library/ or pipelines/, no src/ — only config + dagster core.
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
