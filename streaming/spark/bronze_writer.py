from pyspark.sql.functions import col

from streaming.spark.kafka_stream_reader import read_vehicle_stream


BRONZE_PATH = "s3a://traffic-data-lake/bronze/vehicle_events/"
CHECKPOINT_PATH = (
    "s3a://traffic-data-lake/checkpoints/bronze_vehicle_events/"
)


def start_bronze_stream():
    spark, kafka_df = read_vehicle_stream()

    bronze_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )

    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    return spark, query