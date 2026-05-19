import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"
os.environ["PYSPARK_DRIVER_PYTHON"] = f"{sys.executable}"

load_dotenv()
#links_path = os.getenv("LINKS_SILVER_DEST")
keywords_path = os.getenv("KEY_SILVER_DEST")
#credits_path = os.getenv("CREDITS_SILVER_DEST")

spark = (
    SparkSession.builder
    .appName("IngestLinks")
    .master('local[*]')
    .getOrCreate()
)

df_key = spark.read.parquet(keywords_path)
#df_2 = spark.read.parquet(credits_path)

df = (df_key
    .withColumn("keywords", F.transform("parsed", lambda x: x.getField("name")))
    .drop("parsed")
)
df.show()
df.printSchema()


spark.stop()