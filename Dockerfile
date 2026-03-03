FROM python:3.12-slim

ARG IMAGE_SOURCE

# Copy uv from the official image (faster, no pip needed)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /opt/dagster/app

# Install deps first (cached unless pyproject.toml or uv.lock change)
COPY pyproject.toml uv.lock ./
COPY packages/ ./packages/

# Install the packages
# --frozen ensures uv.lock is respected; --no-cache keeps the image lean
RUN uv sync --no-dev --group cloud --frozen --no-cache

# Add the venv's bin to PATH so dagster (and other scripts) are found
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"

COPY src/ ./src/

# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}