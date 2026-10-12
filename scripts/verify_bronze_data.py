from pyspark.sql.functions import col

from streaming.spark.spark_session import create_spark_session

spark = create_spark_session()

bronze_path = "s3a://traffic-data-lake/bronze/vehicle_events/"

try:
    bronze_df = spark.read.parquet(bronze_path)

    print("\nBronze Schema:")
    bronze_df.printSchema()

    print("\nTotal Bronze Records:")
    print(bronze_df.count())

    print("\nSample Vehicle Events:")
    bronze_df.select(
        "kafka_key",
        "raw_json",
        "kafka_partition",
        "kafka_offset"
    ).show(5, truncate=False)

    print("\nRecords Per Road:")
    bronze_df.groupBy(
        col("kafka_key").alias("road_id")
    ).count().show()

finally:
    spark.stop()