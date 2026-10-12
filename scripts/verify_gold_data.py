from pyspark.sql.functions import sum as spark_sum

from streaming.spark.spark_session import create_spark_session


spark = create_spark_session()

try:
    gold_df = spark.read.parquet(
        "s3a://traffic-data-lake/gold/traffic_analytics/"
    )

    print("\nGold Schema:")
    gold_df.printSchema()

    print("\nTotal Gold Rows:")
    print(gold_df.count())

    print("\nTraffic Analytics:")
    gold_df.orderBy(
        "window_start", "road_id"
    ).show(50, truncate=False)

    print("\nCongestion Distribution:")
    gold_df.groupBy(
        "congestion_status"
    ).count().show()

    print("\nTotal Vehicle Events Represented:")
    gold_df.select(
        spark_sum("event_count").alias("total_events")
    ).show()

finally:
    spark.stop()