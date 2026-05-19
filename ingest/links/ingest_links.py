import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import concat, lit, col

os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"
os.environ["PYSPARK_DRIVER_PYTHON"] = f"{sys.executable}"

links_path = "bronze/links.csv"
silver_path = "ingest/silver/links/"
silver_json_path = "ingest/silver/links_json/"

links_schema = StructType([
    StructField("movieId", IntegerType(), nullable=True),
    StructField("imdbId", StringType(),  nullable=True),
    StructField("tmdbId", IntegerType(),  nullable=True)
])

spark = (
    SparkSession.builder
    .appName("IngestLinks")
    .master('local[*]')
    .getOrCreate()
)

df = spark.read.csv(path=links_path, header=True, schema=links_schema)

df_new = df.withColumn("imdbId", concat(lit("tt"), col("imdbId")))

df_new.show()
df_new.printSchema()

df_new.write.mode("overwrite").parquet(silver_path)
df_new.write.mode("overwrite").json(silver_json_path)


spark.stop()