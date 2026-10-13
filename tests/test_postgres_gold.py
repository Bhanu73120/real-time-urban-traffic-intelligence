
import os
import psycopg2

from streaming.spark.spark_session import create_spark_session

GOLD_PATH = "s3a://traffic-data-lake/gold/traffic_analytics/"


def test_postgres_gold():

    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is not set")

    spark = create_spark_session()
    connection = None

    try:
        # Read Gold data from MinIO
        gold_df = spark.read.parquet(GOLD_PATH)

        expected_rows = gold_df.count()

        expected_events = sum(
            row["event_count"]
            for row in gold_df.select("event_count").collect()
        )

        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            database="traffic_analytics",
            user="traffic_user",
            password=password
        )

        with connection.cursor() as cursor:

            cursor.execute("""
                SELECT
                    COUNT(*),
                    COALESCE(SUM(event_count), 0)
                FROM analytics.traffic_analytics
            """)

            actual_rows, actual_events = cursor.fetchone()

        print("MinIO Gold rows:", expected_rows)
        print("PostgreSQL rows:", actual_rows)

        print("MinIO vehicle events:", expected_events)
        print("PostgreSQL vehicle events:", actual_events)

        assert expected_rows == actual_rows, (
            "Gold row count mismatch"
        )

        assert expected_events == actual_events, (
            "Vehicle event count mismatch"
        )

        print("All PostgreSQL Gold validation tests passed!")

    finally:
        if connection:
            connection.close()

        spark.stop()


if __name__ == "__main__":
    test_postgres_gold()
