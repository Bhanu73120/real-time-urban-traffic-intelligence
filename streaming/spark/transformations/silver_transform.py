from pyspark.sql.functions import col

from streaming.spark.transformations.vehicle_transform import (
    transform_vehicle_events
)
from streaming.spark.transformations.vehicle_quality import (
    validate_vehicle_stream
)


def transform_bronze_to_silver(bronze_df):

    # Convert Bronze columns to the format expected
    # by our existing Day 7 JSON parser.
    source_df = bronze_df.select(
        col("raw_json").cast("binary").alias("value"),
        col("kafka_topic").alias("topic"),
        col("kafka_partition").alias("partition"),
        col("kafka_offset").alias("offset"),
        col("kafka_timestamp").alias("timestamp")
    )

    parsed_df = transform_vehicle_events(source_df)

    validated_df = validate_vehicle_stream(parsed_df)

    return validated_df