import os
import sys
import csv
import io
import ast
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType, StringType, ArrayType,
    StructField, StructType
)
from pyspark.sql.functions import col, udf, size

os.environ["HADOOP_HOME"] = r"D:\hadoop-3.5.5-bin"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = f'"{sys.executable}"'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'"{sys.executable}"'


spark = SparkSession.builder.appName("CreditsParser").getOrCreate()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

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

flat_schema = StructType([
    StructField("cast", StringType(),  nullable=True),
    StructField("crew", StringType(),  nullable=True),
    StructField("id",   IntegerType(), nullable=False),
])


# ---------------------------------------------------------------------------
# CSV line parser
# ---------------------------------------------------------------------------

def parse_line(line: str):
    try:
        reader = csv.reader(
            io.StringIO(line),
            quotechar='"',
            skipinitialspace=True
        )
        row = next(reader)
        if len(row) < 3:
            return None
        return (row[0], row[1], int(row[2].strip()))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Per-object credit parser (robust to mixed/escaped quotes)
# ---------------------------------------------------------------------------

CAST_KEYS = ["cast_id", "character", "credit_id", "gender", "id", "name", "order", "profile_path"]
CREW_KEYS = ["credit_id", "department", "gender", "id", "job", "name", "profile_path"]


def _parse_credits_list(s: str, field_keys: list) -> list:
    """
    Parse a Python-literal list-of-dicts string, processing each dict
    independently so a single malformed entry doesn't drop the whole row.

    Handles the tricky case of CSV-escaped double quotes inside string values
    that also contain single quotes, e.g.:
        'character': ""Nelson - Dithers' Employee""
    """
    if not s:
        return []

    s = s.strip()

    # Strip outer wrapping quote added by CSV reader
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    
    open_braces = s.count('{') - s.count('}')
    if open_braces > 0:
        s += '}' * open_braces
    # Replace CSV-escaped double-quotes ("") with a null-byte placeholder
    # so they don't interfere with splitting or ast.literal_eval.
    s = s.replace('""', '\x00')

    # Extract individual {...} dict strings from the list
    objects = re.findall(r'\{[^{}]*\}', s)

    result = []
    for obj_str in objects:
        try:
            # Restore placeholder as an escaped single quote so
            # ast.literal_eval sees a valid string literal.
            obj_str = obj_str.replace('\x00', "\\'")
            parsed = ast.literal_eval(obj_str)
            if isinstance(parsed, dict):
                result.append({k: parsed.get(k) for k in field_keys})
        except Exception as e:
            # Skip this one bad object; keep everything else
            #obj_str = obj_str.replace('\x00', "\\'")
            #print(obj_str)
            continue

    return result


def parse_cast(s: str):
    return _parse_credits_list(s, CAST_KEYS)


def parse_crew(s: str):
    return _parse_credits_list(s, CREW_KEYS)


cast_udf = udf(parse_cast, cast_schema)
crew_udf = udf(parse_crew, crew_schema)


# ---------------------------------------------------------------------------
# Build DataFrame
# ---------------------------------------------------------------------------

sc = spark.sparkContext

raw_rdd = sc.textFile("bronze_data_sample/credits.csv")
header  = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

parsed_rdd = (
    data_rdd
    .map(parse_line)
    .filter(lambda x: x is not None)
)

df_credits = spark.createDataFrame(parsed_rdd, schema=flat_schema)

df_credits_clean = (
    df_credits
    .withColumn("cast", cast_udf(col("cast")))
    .withColumn("crew", crew_udf(col("crew")))
)

# ---------------------------------------------------------------------------
# Inspect results
# ---------------------------------------------------------------------------

df_credits_clean.printSchema()
df_credits_clean.orderBy("id").show(truncate=80)

print(f"Parsed rows : {df_credits_clean.count()}")
print(f"Raw rows    : {data_rdd.count()}")

print(df_credits_clean.filter(size(col("cast")) == 0).count())
print(df_credits_clean.select(col("id")).filter(size(col("cast")) == 0).collect())


print(df_credits_clean.filter(size(col("crew")) == 0).count())
print(df_credits_clean.select(col("id")).filter(size(col("crew")) == 0).collect())

