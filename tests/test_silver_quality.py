import json
from datetime import datetime, timezone

from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, TimestampType
)
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.silver_transform import (
    transform_bronze_to_silver
)

spark = create_spark_session()

try:
    valid_event = {
        "event_id": "EVT_TEST_001",
        "vehicle_id": "VH_001",
        "road_id": "ROAD_001",
        "event_time": "2026-10-12T06:00:00+00:00",
        "latitude": 17.45,
        "longitude": 78.39,
        "speed_kmph": 35.0,
        "direction": "north",
        "vehicle_type": "car"
    }

    duplicate_event = valid_event.copy()

    invalid_event = valid_event.copy()
    invalid_event["event_id"] = "EVT_TEST_002"
    invalid_event["speed_kmph"] = 250.0

    timestamp = datetime.now(timezone.utc)

    records = [
        (json.dumps(valid_event), "traffic.vehicle.events", 0, 1, timestamp),
        (json.dumps(duplicate_event), "traffic.vehicle.events", 0, 2, timestamp),
        (json.dumps(invalid_event), "traffic.vehicle.events", 0, 3, timestamp)
    ]

    schema = StructType([
        StructField("raw_json", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType())
    ])

    bronze_df = spark.createDataFrame(records, schema)

    validated_df = transform_bronze_to_silver(bronze_df)

    valid_df = validated_df.filter(
        col("validation_status") == "VALID"
    )

    rejected_df = validated_df.filter(
        col("validation_status") == "REJECTED"
    )

    window_spec = Window.partitionBy("event_id").orderBy(
        col("kafka_timestamp").desc(),
        col("kafka_partition").desc(),
        col("kafka_offset").desc()
    )

    deduplicated_df = (
        valid_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
    )

    assert valid_df.count() == 2
    assert rejected_df.count() == 1
    assert deduplicated_df.count() == 1

    print("Valid records before deduplication:", valid_df.count())
    print("Rejected records:", rejected_df.count())
    print("Valid records after deduplication:", deduplicated_df.count())
    print("All Silver quality tests passed.")

finally:
    spark.stop()