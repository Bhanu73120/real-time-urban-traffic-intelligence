
import os

from streaming.spark.spark_session import create_spark_session


def read_vehicle_stream():
    spark = create_spark_session()

    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "traffic.vehicle.events")
        .option("startingOffsets", "earliest")
        .load()
    )

    return spark, kafka_df


def read_incident_stream():
    spark = create_spark_session()

    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "traffic.incident.events")
        .option("startingOffsets", "earliest")
        .load()
    )

    return spark, kafka_df