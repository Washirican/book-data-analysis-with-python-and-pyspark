from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()


DIRECTORY = "./data"
data = spark.read.csv(
    path=os.path.join(DIRECTORY, "sample.csv"),
    sep=",",
    quote="$",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

data.printSchema()
data.show()
