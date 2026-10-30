from pyspark.sql.functions import sum as spark_sum

from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.incident_gold_transform import (
    transform_incident_silver_to_gold,
)


SILVER_PATH = "s3a://traffic-data-lake/silver/incident_events/"
GOLD_PATH = "s3a://traffic-data-lake/gold/incident_summary/"


def main():
    spark = create_spark_session()

    try:
        silver_df = spark.read.parquet(SILVER_PATH)
        silver_count = silver_df.count()

        print("Incident Silver records:", silver_count)

        gold_df = transform_incident_silver_to_gold(silver_df)
        gold_count = gold_df.count()

        print("Incident Gold road records:", gold_count)

        gold_df.show(truncate=False)

        if silver_count == 0:
            total_incidents = 0
        else:
            total_incidents = gold_df.agg(
                spark_sum("total_incidents")
            ).first()[0]

        if total_incidents != silver_count:
            raise ValueError(
                "Incident Gold validation failed. "
                f"Silver count: {silver_count}, "
                f"Gold incident total: {total_incidents}"
            )

        gold_df.write.mode("overwrite").parquet(GOLD_PATH)

        print("Incident Gold validation PASSED.")
        print("Incident Gold pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()