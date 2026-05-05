import os
import sys
import csv
import io
import ast
from pyspark import SparkContext
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"D:\hadoop-3.5.5-bin"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = f'"{sys.executable}"'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'"{sys.executable}"'


from pyspark.sql.types import IntegerType, StringType, ArrayType, StructField, StructType, MapType
from pyspark.sql.functions import from_json, col, regexp_replace, udf


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


sc = spark.sparkContext

raw_rdd = sc.textFile("bronze_data_sample/credits.csv")

# Skip header if present
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

def parse_line(line):
    try:
        # Use QUOTE_NONE to avoid misreading escaped/nested quotes
        reader = csv.reader(
            io.StringIO(line),
            quotechar='"',
            skipinitialspace=True
        )
        row = next(reader)
        if len(row) < 3:
            return None
        cast_str  = row[0]
        crew_str  = row[1]
        movie_id  = int(row[2].strip())
        return (cast_str, crew_str, movie_id)
    except Exception:
        return None

parsed_rdd = data_rdd \
    .map(parse_line) \
    .filter(lambda x: x is not None)

flat_schema = StructType([
    StructField("cast", StringType(),  nullable=True),
    StructField("crew", StringType(),  nullable=True),
    StructField("id",   IntegerType(), nullable=False),
])

df_credits = spark.createDataFrame(parsed_rdd, schema=flat_schema)
df_credits.show()
df_credits.printSchema()





def parse_cast(s):
    if not s:
        return []
    try:
        # Remove the outer wrapping quote if present (CSV artifact)
        s = s.strip()
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]
        
        # Replace CSV-escaped double quotes "" with a placeholder,
        # then convert to proper single-quoted string
        s = s.replace('""', "'")

        parsed = ast.literal_eval(s)
        
        # Ensure every item is a dict, not a tuple
        result = []
        for item in parsed:
            if isinstance(item, dict):
                result.append({
                    "cast_id":      item.get("cast_id"),
                    "character":    item.get("character"),
                    "credit_id":    item.get("credit_id"),
                    "gender":       item.get("gender"),
                    "id":           item.get("id"),
                    "name":         item.get("name"),
                    "order":        item.get("order"),
                    "profile_path": item.get("profile_path"),
                })
        return result
    except Exception:
        return []

def parse_crew(s):
    if not s:
        return []
    try:
        # Remove the outer wrapping quote if present (CSV artifact)
        s = s.strip()
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]
        
        # Replace CSV-escaped double quotes "" with a placeholder,
        # then convert to proper single-quoted string
        s = s.replace('""', "'")
        parsed = ast.literal_eval(s)
        
        # Ensure every item is a dict, not a tuple
        result = []
        for item in parsed:
            if isinstance(item, dict):
                result.append({
                    "cast_id":      item.get("cast_id"),
                    "character":    item.get("character"),
                    "credit_id":    item.get("credit_id"),
                    "gender":       item.get("gender"),
                    "id":           item.get("id"),
                    "name":         item.get("name"),
                    "order":        item.get("order"),
                    "profile_path": item.get("profile_path"),
                })
        return result
    except Exception:
        return []

cast_udf = udf(parse_cast, cast_schema)
crew_udf = udf(parse_crew, crew_schema)

df_credits_clean = df_credits \
    .withColumn("cast", cast_udf(col("cast"))) \
    .withColumn("crew", crew_udf(col("crew")))


df_credits_clean.show()
df_credits_clean.printSchema()

print(df_credits_clean.count())
print(data_rdd.count())

df_credits_clean.orderBy(df_credits_clean["id"]).show()
'''
for row in df_credits.collect():
    print("ID:", row["id"])
    print("CAST:", row["cast"])
    print("CREW:", row["crew"])
    print("-" * 80)
'''