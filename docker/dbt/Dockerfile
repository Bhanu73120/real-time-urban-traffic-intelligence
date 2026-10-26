
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "dbt-postgres==1.9.0"

COPY --chown=1000:1000 dbt/traffic_dbt/ /app/

ENTRYPOINT ["dbt"]
