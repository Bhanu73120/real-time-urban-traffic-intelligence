from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


vehicle_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmph", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicle_type", StringType(), True)
])