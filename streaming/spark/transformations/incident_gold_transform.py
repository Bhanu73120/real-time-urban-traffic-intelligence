from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
)


def transform_incident_silver_to_gold(silver_df):

    gold_df = (
        silver_df
        .groupBy("road_id")
        .agg(
            count("*").alias("total_incidents"),

            spark_sum(
                when(col("severity") == "high", 1).otherwise(0)
            ).alias("high_severity_incidents"),

            spark_sum(
                when(col("severity") == "medium", 1).otherwise(0)
            ).alias("medium_severity_incidents"),

            spark_sum(
                when(col("severity") == "low", 1).otherwise(0)
            ).alias("low_severity_incidents"),

            spark_sum(
                when(col("incident_type") == "accident", 1)
                .otherwise(0)
            ).alias("accidents"),

            spark_sum(
                when(col("incident_type") == "vehicle_breakdown", 1)
                .otherwise(0)
            ).alias("vehicle_breakdowns"),

            spark_sum(
                when(col("incident_type") == "road_closure", 1)
                .otherwise(0)
            ).alias("road_closures"),

            spark_sum(
                when(col("incident_type") == "construction", 1)
                .otherwise(0)
            ).alias("construction_incidents"),
        )
        .orderBy("road_id")
    )

    return gold_df