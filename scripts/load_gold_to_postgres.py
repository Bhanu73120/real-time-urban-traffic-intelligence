import os

from streaming.spark.spark_session import create_spark_session


GOLD_PATH = "s3a://traffic-data-lake/gold/traffic_analytics/"

JDBC_URL = "jdbc:postgresql://localhost:5432/traffic_analytics"

TABLE_NAME = "analytics.traffic_analytics"


def main():

    password = os.environ.get("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD environment variable is not set."
        )

    spark = create_spark_session()

    try:
        gold_df = spark.read.parquet(GOLD_PATH)

        gold_count = gold_df.count()

        print("Gold records:", gold_count)

        # Day 13 uses a full refresh of the analytics table.
        # Keep the existing table and its primary key.
        jdbc_properties = {
            "user": "traffic_user",
            "password": password,
            "driver": "org.postgresql.Driver",
            "truncate": "true"
        }

        (
            gold_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", TABLE_NAME)
            .options(**jdbc_properties)
            .mode("overwrite")
            .save()
        )

        print("Gold analytics loaded into PostgreSQL.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()