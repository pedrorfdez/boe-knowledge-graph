FROM python:3.12-slim

# Set DAGSTER_HOME before any operations that reference it
ENV DAGSTER_HOME=/opt/dagster/dagster_home

RUN pip install uv

WORKDIR /opt/dagster/app

# Copy package definition first (layer cache: reinstall only when deps change)
COPY pyproject.toml .
# Copy application source
COPY pipeline/ pipeline/
# Place dagster config in DAGSTER_HOME (Docker auto-creates the directory)
COPY dagster.yaml /opt/dagster/dagster_home/dagster.yaml

RUN uv pip install --system -e ".[dev]"

# Download spaCy Spanish model
RUN python -m spacy download es_core_news_sm
