from pyspark.sql import SparkSession
from pyspark.sql.functions import col, greatest
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

exo2_4_df = spark.createDataFrame(
[["key", 10_000, 20_000]], ["key", "value1", "value2"]
)

exo2_4_df.printSchema()

try:
    exo2_4_mod = exo2_4_df.select(
    greatest(col("value1"), col("value2")).alias("maximum_value")
    ).select("maximum_value")
except AnalysisException as err:
    print(err)
