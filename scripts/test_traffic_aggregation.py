from datetime import datetime, timezone

from pyspark.sql.functions import col

from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.traffic_aggregation import (
    aggregate_traffic
)
from streaming.spark.transformations.congestion_detection import (
    detect_congestion
)


spark = create_spark_session()

events = [
    ("ROAD_001", 10.0, datetime(2026, 10, 9, 10, 0, 5, tzinfo=timezone.utc), "VALID"),
    ("ROAD_001", 15.0, datetime(2026, 10, 9, 10, 0, 15, tzinfo=timezone.utc), "VALID"),
    ("ROAD_001", 20.0, datetime(2026, 10, 9, 10, 0, 25, tzinfo=timezone.utc), "VALID"),
    ("ROAD_002", 35.0, datetime(2026, 10, 9, 10, 0, 10, tzinfo=timezone.utc), "VALID"),
    ("ROAD_002", 45.0, datetime(2026, 10, 9, 10, 0, 20, tzinfo=timezone.utc), "VALID"),
    ("ROAD_003", 250.0, datetime(2026, 10, 9, 10, 0, 30, tzinfo=timezone.utc), "REJECTED")
]

df = spark.createDataFrame(
    events,
    ["road_id", "speed_kmph", "event_timestamp", "validation_status"]
)

aggregated_df = aggregate_traffic(df)
result_df = detect_congestion(aggregated_df)

result_df.select(
    "road_id",
    "avg_speed_kmph",
    "event_count",
    "congestion_status"
).show(truncate=False)

results = {
    row["road_id"]: row
    for row in result_df.collect()
}

assert results["ROAD_001"]["avg_speed_kmph"] == 15.0
assert results["ROAD_001"]["event_count"] == 3
assert results["ROAD_001"]["congestion_status"] == "MODERATE"

assert results["ROAD_002"]["avg_speed_kmph"] == 40.0
assert results["ROAD_002"]["congestion_status"] == "FREE_FLOW"

assert "ROAD_003" not in results

print("All traffic aggregation tests passed.")

spark.stop()