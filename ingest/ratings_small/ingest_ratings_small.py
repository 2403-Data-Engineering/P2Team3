import os
import sys
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructField, StructType, DoubleType, LongType
# from pyspark.sql.functions import from_json, col, regexp_replace, when, concat, lit
from pyspark.sql.functions import (col, from_unixtime, to_timestamp, row_number, round)
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"
os.environ["PYSPARK_DRIVER_PYTHON"] = f"{sys.executable}"


load_dotenv()
ratings_silver_path= os.getenv("RATINGS_SILVER_PATH")
ratings_bronze_path= os.getenv("RATINGS_BRONZE_PATH")


spark = (
    SparkSession.builder
    .appName("IngestKeywords")
    .master('local[*]')
    .getOrCreate()
)

# -----------------------------
# Define schema using StructType
# -----------------------------
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# -----------------------------
# Read CSV with schema
# -----------------------------
df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(ratings_bronze_path)
)

# ------------------------
# Filter (keep) the latest rating of same movie by same user
# ------------------------  
window_spec = Window.partitionBy("userId", "movieId").orderBy(col("timestamp").desc())

df = df.withColumn("row_num", row_number().over(window_spec))

df = df.filter(col("row_num") == 1).drop("row_num")


# -----------------------------
# Remove rows with null required fields
# -----------------------------
df = df.dropna(
    subset=["userId", "movieId", "rating", "timestamp"]
)

# -----------------------------
# Remove duplicates
# -----------------------------
df = df.dropDuplicates([
    "userId",
    "movieId",
    "timestamp"
])

# -----------------------------
# Filter invalid ratings
# -----------------------------
valid_range = (
    (col("rating") >= 0) &
    (col("rating") <= 5)
)

# valid_increment = (
#     ((col("rating") * 2) % 1 == 0)
# )

valid_increment = (
    (round(col("rating") * 2, 0) == col("rating") * 2)
)

df = df.filter(
    valid_range & valid_increment
)

# -----------------------------
# Convert unix timestamp to datetime
# -----------------------------
df = df.withColumn(
    "rating_time",
    to_timestamp(from_unixtime(col("timestamp")))
)

# -----------------------------
# Remove invalid timestamps
# -----------------------------
df = df.filter(
    col("rating_time").isNotNull()
)

# -----------------------------
# Show cleaned data
# -----------------------------
df.show(truncate=False)

df.printSchema()

# -----------------------------
# Write to parquet
# -----------------------------
df.write.mode("overwrite").parquet(ratings_silver_path)

print(f"Clean parquet written to: {ratings_silver_path}")

spark.stop()