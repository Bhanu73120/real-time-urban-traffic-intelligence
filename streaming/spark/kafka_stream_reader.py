from streaming.spark.spark_session import create_spark_session


def read_vehicle_stream():
    spark = create_spark_session()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "traffic.vehicle.events")
        .option("startingOffsets", "latest")
        .load()
    )

    return spark, kafka_df