from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.silver_transform import (
    transform_bronze_to_silver
)


BRONZE_PATH = "s3a://traffic-data-lake/bronze/vehicle_events/"
SILVER_PATH = "s3a://traffic-data-lake/silver/vehicle_events/"
QUARANTINE_PATH = "s3a://traffic-data-lake/quarantine/vehicle_events/"


def main():

    spark = create_spark_session()

    try:
        bronze_df = spark.read.parquet(BRONZE_PATH)

        print("Bronze records:", bronze_df.count())

        validated_df = transform_bronze_to_silver(bronze_df)

        valid_df = validated_df.filter(
            col("validation_status") == "VALID"
        )

        rejected_df = validated_df.filter(
            col("validation_status") == "REJECTED"
        )

        # Keep the latest record for each event_id.
        window_spec = (
            Window.partitionBy("event_id")
            .orderBy(
                col("kafka_timestamp").desc(),
                col("kafka_partition").desc(),
                col("kafka_offset").desc()
            )
        )

        silver_df = (
            valid_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .select(
                "event_id",
                "vehicle_id",
                "road_id",
                "event_timestamp",
                "latitude",
                "longitude",
                "speed_kmph",
                "direction",
                "vehicle_type",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp"
            )
        )

        print("Silver records:", silver_df.count())
        print("Rejected records:", rejected_df.count())

        silver_df.write.mode("overwrite").parquet(SILVER_PATH)

        rejected_df.write.mode("overwrite").parquet(QUARANTINE_PATH)

        print("Silver and quarantine datasets written successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()