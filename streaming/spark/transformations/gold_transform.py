from pyspark.sql.functions import (
    col,
    window,
    avg,
    count,
    round as spark_round,
    when,
    lit
)


def transform_silver_to_gold(silver_df):

    traffic_df = (
        silver_df
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("road_id")
        )
        .agg(
            spark_round(
                avg("speed_kmph"), 2
            ).alias("avg_speed_kmph"),
            count("*").alias("event_count")
        )
    )

    gold_df = (
        traffic_df
        .withColumn(
            "congestion_status",
            when(
                col("avg_speed_kmph") < 15,
                lit("HEAVY")
            )
            .when(
                col("avg_speed_kmph") < 30,
                lit("MODERATE")
            )
            .otherwise(lit("FREE_FLOW"))
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "road_id",
            "avg_speed_kmph",
            "event_count",
            "congestion_status"
        )
    )

    return gold_df