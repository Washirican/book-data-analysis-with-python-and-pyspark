from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

exo2_2_df = spark.createDataFrame(
[["test", "more test", 10_000_000_000]], ["one", "two", "three"]
)

non_string_count = len([x for x, y in exo2_2_df.dtypes if y != "string"])
print(f"There are {non_string_count}, columns that are not strings in DataFrame")


