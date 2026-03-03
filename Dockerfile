FROM python:3.12-slim

ARG IMAGE_SOURCE

# Copy uv from the official image (faster, no pip needed)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /opt/dagster/app

# Install deps first (cached unless pyproject.toml or uv.lock change)
COPY pyproject.toml uv.lock ./
COPY packages/ ./packages/

# Install the packages globally, instead of venv.
ENV UV_SYSTEM_PYTHON=1
# --frozen ensures uv.lock is respected; --no-cache keeps the image lean
RUN uv sync --no-dev --group cloud --frozen --no-cache

# Copy source after deps to preserve cache on code-only changes
COPY src/ ./src/

# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}