from pyspark.sql.functions import col, lit, when


def validate_vehicle_stream(vehicle_df):

    required_fields = [
        "event_id",
        "vehicle_id",
        "road_id",
        "event_time",
        "direction",
        "vehicle_type"
    ]

    valid_condition = (
        col("speed_kmph").between(0, 200)
        & col("latitude").between(-90, 90)
        & col("longitude").between(-180, 180)
        & col("event_timestamp").isNotNull()
    )

    for field in required_fields:
        valid_condition = (
            valid_condition
            & col(field).isNotNull()
            & (col(field) != "")
        )

    validated_df = vehicle_df.withColumn(
        "validation_status",
        when(valid_condition, lit("VALID"))
        .otherwise(lit("REJECTED"))
    )

    return validated_df