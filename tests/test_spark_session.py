from streaming.spark.spark_session import create_spark_session


spark = create_spark_session()

print("Spark session created successfully.")
print("Spark version:", spark.version)

data = [
    ("ROAD_001", 20),
    ("ROAD_002", 35),
    ("ROAD_003", 15)
]

df = spark.createDataFrame(
    data,
    ["road_id", "speed_kmph"]
)

df.show()

spark.stop()

print("Spark session stopped successfully.")