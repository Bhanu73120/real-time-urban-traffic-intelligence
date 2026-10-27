
FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       procps \
       curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

RUN pip install --no-cache-dir \
    pyspark==3.5.6 \
    psycopg2-binary

COPY streaming/ /app/streaming/
COPY scripts/ /app/scripts/

CMD ["python", "-m", "scripts.load_gold_to_postgres"]
