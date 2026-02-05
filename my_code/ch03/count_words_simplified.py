from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.")
    .config("spark.sql.repl.eagerEval.enabled", "False")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Transformations
results = (
    spark.read.text("./data/gutenberg_books/*.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z]+", 0).alias("word"))
    .filter(F.col("word") != "")
    .groupby("word")
    .count()
)

# Actions
results.orderBy("count", ascending=False).show(10)

results.write.mode("overwrite").csv("./data/simple_count.csv")
results.coalesce(1).write.mode("overwrite").csv(
    "./data/simple_count_single_partition.csv"
)
