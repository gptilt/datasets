FROM python:3.12-slim

# Install uv
RUN pip install uv

WORKDIR /opt/dagster/app

# Copy the workspace configuration and package folders
COPY pyproject.toml uv.lock ./
COPY packages/ ./packages/
COPY src/ ./src/

# Install the workspace (this will install orchestration and all ds-* packages)
RUN uv sync --no-dev

# Set the Python path so Dagster can find 'orchestration'
ENV PYTHONPATH=/opt/dagster/app/src

# Link this image to this repo
LABEL org.opencontainers.image.source=https://github.com/gptilt/datasets