import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"
os.environ["PYSPARK_DRIVER_PYTHON"] = f"{sys.executable}"

load_dotenv()
silver_path = os.getenv("LINKS_SILVER_DEST")

spark = (
    SparkSession.builder
    .appName("TestParquet")
    .master('local[*]')
    .getOrCreate()
)

df = spark.read.parquet(silver_path)
df.cache()
df.show()
df.printSchema()

spark.stop()