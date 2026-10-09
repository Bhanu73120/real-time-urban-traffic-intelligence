import time

from streaming.spark.kafka_stream_reader import read_vehicle_stream
from streaming.spark.transformations.vehicle_transform import (
    transform_vehicle_events
)
from streaming.spark.transformations.vehicle_quality import (
    validate_vehicle_stream
)


spark, kafka_df = read_vehicle_stream()

# Convert Kafka JSON into structured columns
vehicle_df = transform_vehicle_events(kafka_df)

# Apply data-quality rules
validated_df = validate_vehicle_stream(vehicle_df)

# Display important fields
output_df = validated_df.select(
    "event_id",
    "road_id",
    "speed_kmph",
    "event_timestamp",
    "validation_status",
    "kafka_partition",
    "kafka_offset"
)

print("Spark traffic transformation stream started.")
print("Press Ctrl+C to stop.")

query = None

try:
    query = (
        output_df.writeStream
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