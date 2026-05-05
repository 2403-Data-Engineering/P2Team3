from pyspark.sql import SparkSession
import os, sys

os.environ["PYSPARK_PYTHON"] = f'{sys.executable}'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'{sys.executable}'

spark = (
    SparkSession.builder
    .appName("HelloSparkApp")
    .master('local[*]')
    .getOrCreate()
)

df = spark.sql("SELECT 'Hello Spark!' as test")
df.cache()
df.show()
spark.stop()