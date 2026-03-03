FROM python:3.12-slim

ARG IMAGE_SOURCE

# Install uv
RUN pip install uv

WORKDIR /opt/dagster/app

# Copy the workspace configuration and package folders
COPY pyproject.toml uv.lock ./
COPY packages/ ./packages/
COPY src/ ./src/

# Install the workspace (this will install orchestration and all ds-* packages)
# '--system' installs the packages globally, instead of the virtual environment.
RUN uv sync --no-dev --group cloud --system

# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src

# Link this image to this repo
LABEL org.opencontainers.image.source=${IMAGE_SOURCE}