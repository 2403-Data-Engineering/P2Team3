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
from pyspark.sql.functions import col, udf, size, explode, explode_outer, first, last, collect_list, struct, count, sum

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
# Per-object credit parser 
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
    
    #Try to handle bad json by trying to close it
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
script_dir = os.path.dirname(os.path.abspath(__file__))

bronze_path = os.path.join(script_dir, "..", "bronze", "bronze_data_sample", "credits.csv")

raw_rdd = sc.textFile(bronze_path)
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

print("col with multiple ids:")
print(df_credits_clean.groupBy("id").agg(count("id").alias("count")).orderBy(col("count"), ascending=False).filter(col("count") > 1).agg(sum(col("count")).alias("total_repeated_ids")).collect())


print(f"Parsed rows : {df_credits_clean.count()}")
print(f"Raw rows    : {data_rdd.count()}")

print("Cols with no cast:")
print(df_credits_clean.filter(size(col("cast")) == 0).count())
#print(df_credits_clean.select(col("id")).filter(size(col("cast")) == 0).collect())

print("Cols with no crew:")
print(df_credits_clean.filter(size(col("crew")) == 0).count())
#print(df_credits_clean.select(col("id")).filter(size(col("crew")) == 0).collect())


# ---------------------------------------------------------------------------
# Merge smae movie id, while also getting longgest information - cast
# ---------------------------------------------------------------------------

@udf(StringType())
def pick_longest_data(values:list):
    candidates = [v for v in (values or []) if v is not None]
    return max(candidates, key=len) if candidates else None


#Expand columns, and group by id and credit_id, and find the first element in the group that is not null
#or make a list out of it to be reduced into the longgest value
df_cast_merge = df_credits_clean.select(col("id"), explode_outer(col("cast")).alias("c"))\
        .groupBy(col("id"), col("c.credit_id"))\
        .agg(
                first("c.cast_id",      ignorenulls=True).alias("cast_id"),
                collect_list("c.character").alias("all_characters"),
                collect_list("c.name").alias("all_names"),
                collect_list("c.profile_path").alias("all_profiles"),
                first("c.gender",       ignorenulls=True).alias("gender"),
                first("c.id",           ignorenulls=True).alias("person_id"),
                first("c.order",        ignorenulls=True).alias("order"),
        )

#look for longgest: name, character, and profile_path

df_cast_merge = df_cast_merge \
    .withColumn("character",   pick_longest_data("all_characters")) \
    .withColumn("name",        pick_longest_data("all_names")) \
    .withColumn("profile_path",pick_longest_data("all_profiles")) \
    .drop("all_characters", "all_names", "all_profiles")


#add structure back to data
df_cast_merge = df_cast_merge.select(
    col("id"),
    struct(
        col("cast_id"),
        col("character"),
        col("name"),
        col("profile_path"),
        col("gender"),
        col("person_id"),
        col("order"),
        col("credit_id")
    ).alias("c")
)


#make same movie id collumns back to array
df_cast_merge = df_cast_merge.groupBy("id").agg(
    collect_list("c").alias("cast")
)




#df_cast_merge.show()
#df_cast_merge.printSchema()



# ---------------------------------------------------------------------------
# Merge smae movie id, while also getting longgest information - crew
# ---------------------------------------------------------------------------


#Expand columns, and group by id and credit_id, and find the first element in the group that is not null
#or make a list out of it to be reduced into the longgest value
df_crew_merge = df_credits_clean.select(col("id"), explode_outer(col("crew")).alias("c"))\
        .groupBy(col("id"), col("c.credit_id"))\
        .agg(
                collect_list("c.department").alias("all_departments"),
                collect_list("c.name").alias("all_names"),
                collect_list("c.profile_path").alias("all_profiles"),
                collect_list("c.job").alias("all_jobs"),
                first("c.gender",       ignorenulls=True).alias("gender"),
                first("c.id",           ignorenulls=True).alias("person_id"),
        )

#look for longgest: name, character, and profile_path

df_crew_merge = df_crew_merge \
    .withColumn("department",   pick_longest_data("all_departments")) \
    .withColumn("name",         pick_longest_data("all_names")) \
    .withColumn("profile_path", pick_longest_data("all_profiles"))\
    .withColumn("job",          pick_longest_data("all_jobs")) \
    .drop("all_characters", "all_names", "all_profiles", "all_jobs", "all_departments")


#add structure back to data
df_crew_merge = df_crew_merge.select(
    col("id"),
    struct(
        col("department"),
        col("job"),
        col("name"),
        col("profile_path"),
        col("gender"),
        col("person_id"),
        col("credit_id")
    ).alias("c")
)


#make same movie id collumns back to array
df_crew_merge = df_crew_merge.groupBy("id").agg(
    collect_list("c").alias("crew")
)

#df_crew_merge.show()
#df_crew_merge.printSchema()


df_final = df_crew_merge.join(df_cast_merge, on="id")
#df_final.show()
#df_final.printSchema()
print("number of columns")
print(df_final.count())
df_final.write.parquet("ingest/silver/credits", mode="overwrite")