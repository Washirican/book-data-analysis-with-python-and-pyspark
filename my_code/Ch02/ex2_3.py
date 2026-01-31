from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length

spark = SparkSession.builder.getOrCreate()

exo2_3_df = (
spark.read.text("./data/gutenberg_books/1342-0.txt")
.select(length(col("value")).alias("number_of_characters"))
# .withColumnRenamed("length(value)", "number_of_char")
)

exo2_3_df.show(5, truncate=False)
