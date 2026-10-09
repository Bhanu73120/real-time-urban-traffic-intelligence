from pyspark.sql.functions import (
    col,
    window,
    avg,
    count,
    round as spark_round
)


def aggregate_traffic(vehicle_df):

    valid_df = vehicle_df.filter(
        col("validation_status") == "VALID"
    )

    traffic_df = (
        valid_df
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(
            window(
                col("event_timestamp"),
                "1 minute"
            ),
            col("road_id")
        )
        .agg(
            spark_round(
                avg("speed_kmph"), 2
            ).alias("avg_speed_kmph"),

            count("*").alias("event_count")
        )
    )

    return traffic_df