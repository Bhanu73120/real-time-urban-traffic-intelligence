
import argparse

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.incident_silver_transform import (
    transform_incident_bronze_to_silver,
)


BRONZE_PATH = "s3a://traffic-data-lake/bronze/incident_events/"
SILVER_PATH = "s3a://traffic-data-lake/silver/incident_events/"
QUARANTINE_PATH = "s3a://traffic-data-lake/quarantine/incident_events/"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process incident events for one Airflow run."
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="Current Airflow demo run ID.",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    current_run_id = args.run_id

    print(f"Processing incidents for run: {current_run_id}")

    spark = create_spark_session()

    try:
        bronze_df = spark.read.parquet(BRONZE_PATH)

        validated_df = transform_incident_bronze_to_silver(
            bronze_df
        )

        # Process only incidents from this Airflow run.
        current_df = validated_df.filter(
            col("run_id") == current_run_id
        )

        print("Current run incident records:", current_df.count())

        valid_df = current_df.filter(
            col("validation_status") == "VALID"
        )

        rejected_df = current_df.filter(
            col("validation_status") == "REJECTED"
        )

        # Deduplicate by incident_id.
        window_spec = (
            Window.partitionBy("incident_id")
            .orderBy(
                col("kafka_timestamp").desc(),
                col("kafka_partition").desc(),
                col("kafka_offset").desc(),
            )
        )

        silver_df = (
            valid_df
            .withColumn(
                "row_num",
                row_number().over(window_spec)
            )
            .filter(col("row_num") == 1)
            .drop("row_num", "validation_status")
        )

        silver_count = silver_df.count()
        rejected_count = rejected_df.count()

        print("Incident Silver records:", silver_count)
        print("Rejected incident records:", rejected_count)

        if rejected_count > 0:
            raise ValueError(
                f"Incident validation failed: "
                f"{rejected_count} rejected records."
            )

        # Zero incidents is valid for a run.
        silver_df.write.mode("overwrite").parquet(
            SILVER_PATH
        )

        rejected_df.write.mode("overwrite").parquet(
            QUARANTINE_PATH
        )

        print("Incident Silver pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
