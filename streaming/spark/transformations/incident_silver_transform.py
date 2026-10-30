from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    trim,
    when,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


INCIDENT_SCHEMA = StructType([
    StructField("incident_id", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("run_id", StringType(), True),
])


def transform_incident_bronze_to_silver(bronze_df):

    # Step 1: Parse the raw incident JSON.
    parsed_df = bronze_df.withColumn(
        "incident",
        from_json(col("raw_json"), INCIDENT_SCHEMA)
    )

    # Step 2: Extract incident fields.
    silver_df = parsed_df.select(
        trim(col("incident.run_id")).alias("run_id"),
        trim(col("incident.incident_id")).alias("incident_id"),
        trim(col("incident.road_id")).alias("road_id"),
        trim(col("incident.incident_type")).alias("incident_type"),
        trim(col("incident.severity")).alias("severity"),
        to_timestamp(
            col("incident.event_time")
        ).alias("event_timestamp"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
    )

    # Step 3: Validate incident records.
    valid_condition = (
        col("incident_id").isNotNull()
        & (col("incident_id") != "")
        & col("road_id").isNotNull()
        & (col("road_id") != "")
        & col("incident_type").isin(
            "accident",
            "vehicle_breakdown",
            "road_closure",
            "construction",
        )
        & col("severity").isin(
            "low",
            "medium",
            "high",
        )
        & col("event_timestamp").isNotNull()
    )

    # Step 4: Mark records as VALID or REJECTED.
    validated_df = silver_df.withColumn(
        "validation_status",
        when(
            valid_condition,
            "VALID"
        ).otherwise("REJECTED")
    )

    return validated_df