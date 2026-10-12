from streaming.spark.spark_session import create_spark_session
from streaming.spark.transformations.gold_transform import (
    transform_silver_to_gold
)


SILVER_PATH = "s3a://traffic-data-lake/silver/vehicle_events/"
GOLD_PATH = "s3a://traffic-data-lake/gold/traffic_analytics/"


def main():

    spark = create_spark_session()

    try:
        print("Reading Silver vehicle events...")

        silver_df = spark.read.parquet(SILVER_PATH)

        print("Silver records:", silver_df.count())

        gold_df = transform_silver_to_gold(silver_df)

        print("\nGold traffic analytics:")

        gold_df.orderBy(
            "window_start", "road_id"
        ).show(50, truncate=False)

        print("Gold records:", gold_df.count())

        gold_df.write.mode("overwrite").parquet(GOLD_PATH)

        print("\nGold data written successfully to MinIO.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()