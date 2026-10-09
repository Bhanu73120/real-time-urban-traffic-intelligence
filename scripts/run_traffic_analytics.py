import time

from pyspark.sql.functions import col
from streaming.spark.checkpoint_config import get_checkpoint_path
from streaming.spark.kafka_stream_reader import read_vehicle_stream
from streaming.spark.transformations.vehicle_transform import (
    transform_vehicle_events
)
from streaming.spark.transformations.vehicle_quality import (
    validate_vehicle_stream
)
from streaming.spark.transformations.traffic_aggregation import (
    aggregate_traffic
)
from streaming.spark.transformations.congestion_detection import (
    detect_congestion
)


spark, kafka_df = read_vehicle_stream()

vehicle_df = transform_vehicle_events(kafka_df)
validated_df = validate_vehicle_stream(vehicle_df)

traffic_df = aggregate_traffic(validated_df)
congestion_df = detect_congestion(traffic_df)

output_df = congestion_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "road_id",
    "avg_speed_kmph",
    "event_count",
    "congestion_status"
)

print("Real-time traffic analytics started.")
print("Press Ctrl+C to stop.")

query = None

try:
    query = (
        output_df.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("checkpointLocation", get_checkpoint_path())
        .trigger(processingTime="5 seconds")
        .start()
    )

    while query.isActive:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping traffic analytics...")

finally:
    if query is not None and query.isActive:
        query.stop()

    spark.stop()
    print("Spark session stopped.")