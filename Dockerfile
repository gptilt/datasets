FROM python:3.12-slim

ARG IMAGE_SOURCE

# Copy uv from the official image (faster, no pip needed)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /opt/dagster/app

# Copy dependency manifests
COPY pyproject.toml uv.lock ./
# Copy workspace packages and the root source code
COPY packages/ ./packages/
COPY src/ ./src/
# Instance + workspace config. DAGSTER_HOME points here, so the user-code gRPC
# server, webserver, and daemon all read the same dagster.yaml; the webserver and
# daemon find the code via workspace.yaml.
COPY dagster.yaml workspace.yaml ./

# Install the packages as standard, immutable wheels
# --frozen ensures uv.lock is respected
# --no-cache keeps the image lean
# --no-editable guarantees no symlinks are used (true production artifact)
# --group server pulls in dagster-webserver + dagster-postgres so this one image
# can run all three OSS roles (user-code gRPC server, webserver, daemon).
RUN uv sync --no-dev --extra private --group server --frozen --no-cache --no-editable

# Add the venv's bin to PATH so dagster is found
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src
# DAGSTER_HOME = dir holding dagster.yaml (instance config) for every OSS role.
ENV DAGSTER_HOME=/opt/dagster/app

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}