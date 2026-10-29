import os
from pyspark.sql.functions import col

from streaming.spark.kafka_stream_reader import read_vehicle_stream


BRONZE_PATH = "s3a://traffic-data-lake/bronze/airflow_vehicle_events_v2/"

CHECKPOINT_PATH = os.getenv(
    "BRONZE_CHECKPOINT_PATH",
    "s3a://traffic-data-lake/checkpoints/airflow_vehicle_events_v3/",
)

def start_bronze_stream(available_now=False):
    spark, kafka_df = read_vehicle_stream()

    bronze_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    writer = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
    )

    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")

    query = writer.start()

    return spark, query
