from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()


DIRECTORY = "./data/broadcast_logs"
logs_clean = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

# Keep only columns that do not end in 'ID'
logs_clean = logs.drop(*[c for c in logs.columns if not c.endswith('ID')])


logs_clean.printSchema()
