from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.")
    .config("spark.sql.repl.eagerEval.enabled", "False")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("OFF")

# Transformations
results = (
    spark.read.text("./data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z]+", 0).alias("word"))
    .filter(F.col("word") != "")
    .groupby("word")
    #    .select(F.substring(F.col("word"), 1, 1).alias("first_letter"))
    #    .groupBy(F.col("first_letter"))
    .count()
    .orderBy(F.col("count"), ascending=False)
    .withColumn(
        "first_letter_vowel",
        F.substring(F.col("word"), 1, 1).isin(["a", "e", "i", "o", "u"]),
    )
    .groupBy(F.col("first_letter_vowel"))
    .sum()
    # .show()
)
# Actions
results.show(5)
