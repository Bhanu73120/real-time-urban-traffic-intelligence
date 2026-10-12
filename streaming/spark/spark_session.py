import os

from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("UrbanTrafficStreaming")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.port", "4040")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ])
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY", "minioadmin123")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark