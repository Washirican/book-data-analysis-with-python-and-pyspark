from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract

spark = (SparkSession
.builder
.appName("Analyzing the vocabulary of Pride and Prejudice.")
.config("spark.sql.repl.eagerEval.enabled", "True")
.getOrCreate())

spark.sparkContext.setLogLevel("OFF")

book = spark.read.text("./data/gutenberg_books/1342-0.txt")

# book.printSchema()

# book.show(10, truncate=False)

lines = book.select(split(col("value"), ' ').alias("line"))

words = lines.select(explode(col("line")).alias("word"))

words_lower = words.select(lower(col("word")).alias("word_lower"))

words_clean = words_lower.select(
    regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word")
)

words_nonull = words_clean.filter(col("word") != "")

groups = words_nonull.groupBy(col("word"))

result = groups.count()
