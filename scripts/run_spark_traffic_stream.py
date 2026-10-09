import time

from pyspark.sql.functions import col
from streaming.spark.kafka_stream_reader import read_vehicle_stream


spark, kafka_df = read_vehicle_stream()

traffic_df = kafka_df.select(
    col("key").cast("string").alias("road_key"),
    col("value").cast("string").alias("event_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

print("Spark Kafka traffic stream started.")
print("Press Ctrl+C to stop.")

query = None

try:
    query = (
        traffic_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    while query.isActive:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping Spark traffic stream...")

finally:
    if query is not None and query.isActive:
        query.stop()

    spark.stop()
    print("Spark session stopped.")