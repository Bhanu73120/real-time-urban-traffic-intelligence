from streaming.spark.spark_session import create_spark_session

spark = create_spark_session()

try:
    silver_df = spark.read.parquet(
        "s3a://traffic-data-lake/silver/vehicle_events/"
    )

    print("\nSilver Schema:")
    silver_df.printSchema()

    print("\nTotal Silver Records:")
    print(silver_df.count())

    print("\nSample Clean Vehicle Events:")
    silver_df.select(
        "event_id",
        "road_id",
        "speed_kmph",
        "event_timestamp"
    ).show(10, truncate=False)

    print("\nRecords Per Road:")
    silver_df.groupBy("road_id").count().show()

finally:
    spark.stop()