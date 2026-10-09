import json
from datetime import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    BinaryType,
    StringType,
    IntegerType,
    LongType,
    TimestampType
)

from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.vehicle_transform import (
    transform_vehicle_events
)
from streaming.spark.transformations.vehicle_quality import (
    validate_vehicle_stream
)


spark = create_spark_session()

valid_event = {
    "event_id": "EVT_001",
    "vehicle_id": "VH_1001",
    "road_id": "ROAD_001",
    "event_time": "2026-10-09T05:00:00+00:00",
    "latitude": 17.4435,
    "longitude": 78.3772,
    "speed_kmph": 35.5,
    "direction": "north",
    "vehicle_type": "car"
}

invalid_event = valid_event.copy()
invalid_event["event_id"] = "EVT_002"
invalid_event["speed_kmph"] = 250.0

kafka_schema = StructType([
    StructField("value", BinaryType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True)
])

records = [
    (
        json.dumps(valid_event).encode("utf-8"),
        "traffic.vehicle.events",
        0,
        1,
        datetime.now()
    ),
    (
        json.dumps(invalid_event).encode("utf-8"),
        "traffic.vehicle.events",
        0,
        2,
        datetime.now()
    )
]

kafka_df = spark.createDataFrame(
    records,
    schema=kafka_schema
)

transformed_df = transform_vehicle_events(kafka_df)
validated_df = validate_vehicle_stream(transformed_df)

validated_df.select(
    "event_id",
    "road_id",
    "speed_kmph",
    "validation_status"
).show(truncate=False)

results = {
    row["event_id"]: row["validation_status"]
    for row in validated_df.select(
        "event_id", "validation_status"
    ).collect()
}

assert results["EVT_001"] == "VALID"
assert results["EVT_002"] == "REJECTED"

print("All Spark vehicle transformation tests passed.")

spark.stop()