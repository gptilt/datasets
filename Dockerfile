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

# Install the packages as standard, immutable wheels
# --frozen ensures uv.lock is respected
# --no-cache keeps the image lean
# --no-editable guarantees no symlinks are used (true production artifact)
RUN uv sync --no-dev --group cloud --frozen --no-cache --no-editable

# Add the venv's bin to PATH so dagster is found
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}