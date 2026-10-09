from pyspark.sql.functions import col, when, lit


def detect_congestion(traffic_df):

    result_df = traffic_df.withColumn(
        "congestion_status",
        when(
            col("avg_speed_kmph") < 15,
            lit("HEAVY")
        )
        .when(
            col("avg_speed_kmph") < 30,
            lit("MODERATE")
        )
        .otherwise(
            lit("FREE_FLOW")
        )
    )

    return result_df