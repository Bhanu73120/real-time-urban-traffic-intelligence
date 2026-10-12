import time

from streaming.spark.bronze_writer import start_bronze_stream


spark, query = start_bronze_stream()

print("Bronze ingestion started.")
print("Writing raw Kafka vehicle events to MinIO as Parquet.")
print("Press Ctrl+C to stop.")

try:
    while query.isActive:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping Bronze ingestion...")

finally:
    if query.isActive:
        query.stop()

    spark.stop()
    print("Bronze ingestion stopped.")