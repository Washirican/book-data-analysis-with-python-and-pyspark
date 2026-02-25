from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()


DIRECTORY = "./data/broadcast_logs"
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

# Removes unwanted columns from DataFrame
logs = logs.drop("BroadcastLogID", "SequenceNO")

# Adds new column for duration data in seconds
logs = logs.withColumn(
    "Duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

# Rename all columns to lower case
# logs = logs.toDF(*[x.lower() for x in logs.columns])


logs.printSchema()
