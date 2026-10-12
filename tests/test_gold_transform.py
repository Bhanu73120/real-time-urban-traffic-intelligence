from datetime import datetime

from pyspark.sql import SparkSession

from streaming.spark.transformations.gold_transform import (
    transform_silver_to_gold
)


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("GoldTransformationTest")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        rows = [
            ("ROAD_001", datetime(2026, 10, 12, 10, 0, 5), 10.0),
            ("ROAD_001", datetime(2026, 10, 12, 10, 0, 15), 12.0),
            ("ROAD_002", datetime(2026, 10, 12, 10, 0, 20), 20.0),
            ("ROAD_002", datetime(2026, 10, 12, 10, 0, 30), 24.0),
            ("ROAD_003", datetime(2026, 10, 12, 10, 0, 40), 40.0),
            ("ROAD_003", datetime(2026, 10, 12, 10, 1, 10), 35.0),
        ]

        schema = "road_id string, event_timestamp timestamp, speed_kmph double"

        silver_df = spark.createDataFrame(rows, schema)

        gold_df = transform_silver_to_gold(silver_df)

        results = {
            (row["road_id"], row["window_start"].minute): row
            for row in gold_df.collect()
        }

        assert len(results) == 4

        assert results[("ROAD_001", 0)]["avg_speed_kmph"] == 11.0
        assert results[("ROAD_001", 0)]["congestion_status"] == "HEAVY"
        assert results[("ROAD_001", 0)]["event_count"] == 2

        assert results[("ROAD_002", 0)]["avg_speed_kmph"] == 22.0
        assert results[("ROAD_002", 0)]["congestion_status"] == "MODERATE"

        assert results[("ROAD_003", 0)]["congestion_status"] == "FREE_FLOW"
        assert results[("ROAD_003", 1)]["event_count"] == 1

        assert sum(row["event_count"] for row in results.values()) == 6

        print("All Gold transformation tests passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()