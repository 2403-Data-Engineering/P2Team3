import os
import sys
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructField, StructType
from pyspark.sql.functions import from_json, col, regexp_replace, when, concat, lit

os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"
os.environ["PYSPARK_DRIVER_PYTHON"] = f"{sys.executable}"

load_dotenv()
keyword_path= os.getenv("KEYWORD_PATH")
output_path = os.getenv("KEY_TEMPCSV")
silver_path = os.getenv("KEY_SILVER_DEST")



def clean_csv(inputpath, outputpath):
    text = ''
    with open(inputpath, 'r', encoding='utf-8') as file:
        text = file.read()

    text = re.sub(r'""([^"]+)""', r"'\1'", text)
    with open(outputpath, 'w', encoding='utf-8') as file:
        file.write(text)

spark = (
    SparkSession.builder
    .appName("IngestKeywords")
    .master('local[*]')
    .getOrCreate()
)

singlekey_schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(),  nullable=True),
])

keywords_schema = ArrayType(singlekey_schema)

flat_schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("keywords", StringType(),  nullable=True)
])

clean_csv(keyword_path, output_path)

df = spark.read.csv(path=output_path, header=True, schema=flat_schema)
df.orderBy('id').show()

df_new = (
    df
    .withColumn("keywords", regexp_replace(col("keywords"), r"(?<=[\[{,:])\s*'", '"'))
    .withColumn("keywords", regexp_replace(col("keywords"), r"'\s*(?=[\]},:])", '"'))
    .withColumn("keywords", regexp_replace(col("keywords"), r'\bNone\b', 'null'))
    .withColumn("keywords", regexp_replace(col("keywords"), r"\\xa0", ''))
    .withColumn(
    "keywords",
    when(
        ~col("keywords").startswith("["),
        concat(lit("["), col("keywords"), lit("]"))
    ).otherwise(col("keywords")))
)

df_new2 = (
    df_new
    .withColumn("parsed", from_json(col("keywords"), keywords_schema))
    .filter(col("keywords") != '[]')
    .dropna()
    .distinct()
    .orderBy('id')
    .select('id', 'parsed')
    )
df_new2.show()
df_new2.printSchema()

df_new2.write.mode("overwrite").parquet(silver_path)

spark.stop()
