FROM python:3.12-slim

ARG IMAGE_SOURCE

# Set the Git metadata as build arguments
ARG DAGSTER_CLOUD_GIT_SHA
ARG DAGSTER_CLOUD_GIT_BRANCH
ARG DAGSTER_CLOUD_GIT_URL
ARG DAGSTER_CLOUD_GIT_TAG
# Set them as environment variables so the Python process can read them at runtime
ENV DAGSTER_CLOUD_GIT_SHA=$DAGSTER_CLOUD_GIT_SHA \
    DAGSTER_CLOUD_GIT_BRANCH=$DAGSTER_CLOUD_GIT_BRANCH \
    DAGSTER_CLOUD_GIT_URL=$DAGSTER_CLOUD_GIT_URL \
    DAGSTER_CLOUD_GIT_TAG=$DAGSTER_CLOUD_GIT_TAG

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

# Copy everything, not just src/
COPY . .

# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src
ENV DAGSTER_CLOUD_GIT_REPO_DIR=/opt/dagster/app

LABEL org.opencontainers.image.source=${IMAGE_SOURCE}