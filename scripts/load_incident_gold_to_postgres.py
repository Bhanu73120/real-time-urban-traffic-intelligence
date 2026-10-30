
import os

from pyspark.sql.functions import col

from streaming.spark.spark_session import create_spark_session


GOLD_PATH = "s3a://traffic-data-lake/gold/incident_summary/"
TABLE_NAME = "analytics.incident_summary"

JDBC_URL = os.getenv(
    "TRAFFIC_JDBC_URL",
    "jdbc:postgresql://localhost:5432/traffic_analytics",
)


def main():
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is not set.")

    spark = create_spark_session()

    try:
        gold_df = spark.read.parquet(GOLD_PATH)

        print("Incident Gold road records:", gold_df.count())

        gold_df = gold_df.withColumn(
            "total_incidents",
            col("total_incidents").cast("long")
        )

        jdbc_properties = {
            "user": "traffic_user",
            "password": password,
            "driver": "org.postgresql.Driver",
        }

        (
            gold_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", TABLE_NAME)
            .option("truncate", "true")
            .options(**jdbc_properties)
            .mode("overwrite")
            .save()
        )

        print("Incident summary loaded into PostgreSQL.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
