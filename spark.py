import os
import sys
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"D:\hadoop-3.5.5-bin"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = f'"{sys.executable}"'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'"{sys.executable}"'


from pyspark.sql.types import IntegerType, StringType, ArrayType, StructField, StructType, MapType
from pyspark.sql.functions import from_json, col, regexp_replace


spark = SparkSession.builder.appName("WordCount").getOrCreate()


cast_schema = ArrayType(StructType([
    StructField("cast_id",      IntegerType(), nullable=True),
    StructField("character",    StringType(),  nullable=True),
    StructField("credit_id",    StringType(),  nullable=True),
    StructField("gender",       IntegerType(), nullable=True),
    StructField("id",           IntegerType(), nullable=True),
    StructField("name",         StringType(),  nullable=True),
    StructField("order",        IntegerType(), nullable=True),
    StructField("profile_path", StringType(),  nullable=True),
]))

crew_schema = ArrayType(StructType([
    StructField("credit_id",    StringType(),  nullable=True),
    StructField("department",   StringType(),  nullable=True),
    StructField("gender",       IntegerType(), nullable=True),
    StructField("id",           IntegerType(), nullable=True),
    StructField("job",          StringType(),  nullable=True),
    StructField("name",         StringType(),  nullable=True),
    StructField("profile_path", StringType(),  nullable=True),
]))

credits_schema = StructType([
    StructField("cast", ArrayType(cast_schema), nullable=True),
    StructField("crew", ArrayType(crew_schema), nullable=True),
    StructField("id",   IntegerType(),          nullable=False),
])

flat_schema = StructType([
    StructField("cast", StringType(), nullable=True),
    StructField("crew", StringType(), nullable=True),
    StructField("id",   IntegerType(), nullable=False),
])

df_credits = spark.read.csv(path="", header=True, schema=flat_schema)
df_credits.show()
df_credits.printSchema()


df_credits = df_credits \
    .withColumn("cast", regexp_replace(col("cast"), "'", '"')) \
    .withColumn("cast", regexp_replace(col("cast"), '""', '"')) \
    .withColumn("crew", regexp_replace(col("crew"), "'", '"')) \
    .withColumn("crew", regexp_replace(col("crew"), '""', '"')) \
    .withColumn("cast", from_json(col("cast"), cast_schema)) \
    .withColumn("crew", from_json(col("crew"), crew_schema))



df_credits.show()
df_credits.printSchema()
df_credits.count()
