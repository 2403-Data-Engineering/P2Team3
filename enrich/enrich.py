import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"
os.environ["PYSPARK_DRIVER_PYTHON"] = f"{sys.executable}"

load_dotenv()
links_path = os.getenv("LINKS_SILVER_DEST")
keywords_path = os.getenv("KEY_SILVER_DEST")
credits_path = os.getenv("CREDITS_SILVER_DEST")

spark = (
    SparkSession.builder
    .appName("IngestLinks")
    .master('local[*]')
    .getOrCreate()
)

df_1 = spark.read.parquet(keywords_path)
df_2 = spark.read.parquet(credits_path)

df_1.show()
df_2.show()

joined_df = df_1.join(df_2, on='id', how="outer")
joined_df.show()
joined_df.printSchema()

spark.stop()

