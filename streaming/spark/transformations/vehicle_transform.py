from pyspark.sql.functions import col, from_json, to_timestamp

from streaming.spark.schemas.vehicle_schema import vehicle_schema


def transform_vehicle_events(kafka_df):

    parsed_df = kafka_df.withColumn(
        "parsed_event",
        from_json(
            col("value").cast("string"),
            vehicle_schema
        )
    )

    vehicle_df = parsed_df.select(
        col("parsed_event.*"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_json")
    )

    vehicle_df = vehicle_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    )

    return vehicle_df