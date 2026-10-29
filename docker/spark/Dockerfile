
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
    psycopg2-binary \
    confluent-kafka

# Resolve Spark dependencies while building the image.
RUN python - <<'PY'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("DownloadSparkDependencies")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.postgresql:postgresql:42.7.4",
        ])
    )
    .getOrCreate()
)

spark.stop()
PY

COPY streaming/ /app/streaming/
COPY simulator/ /app/simulator/
COPY scripts/ /app/scripts/
COPY data/reference/ /app/data/reference/

CMD ["python", "-m", "scripts.load_gold_to_postgres"]
