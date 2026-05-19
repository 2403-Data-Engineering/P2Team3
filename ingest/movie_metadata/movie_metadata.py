import os, sys, csv, io, ast, re, json_repair, ftfy

from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = f'{sys.executable}'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'{sys.executable}'

from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, ArrayType, StructField, StructType, MapType, BooleanType, DoubleType, DateType
from pyspark.sql.functions import col, udf, to_date, first, collect_list


spark = SparkSession.builder.appName("MovieMetadata").getOrCreate()

belong_schema = ArrayType(StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("poster_path", StringType(), nullable= True),
    StructField("backdrop_path", StringType(), nullable=True)
]))

genre_schema = ArrayType(StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
]))

production_company_schema = ArrayType(StructType([
    StructField("name", StringType(), nullable=True),
    StructField("id", IntegerType(), nullable=True)
]))

production_country_schema = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True)
]))

language_schema = ArrayType(StructType([
    StructField("iso_639_1", StringType(),nullable=True),
    StructField("name", StringType(), nullable=True)
]))



meta_schema = StructType([
    
    StructField("adult", BooleanType(), nullable=True), #0
    
    StructField("belongs_to_collection", StringType(), nullable=True),
    
    #StructField("budget", DoubleType(), nullable=True), #2 ***************

    StructField("genres", StringType(), nullable=True),
    
    # StructField("homepage", StringType(), nullable=True),#4 *******
    
    StructField("id", IntegerType(), nullable=False),
    
    # StructField("imdb_id", StringType(), nullable=True),#6 ******
    # StructField("original_language", StringType(), nullable=True), # *****
    # StructField("original_title", StringType(),nullable=True), #8 **********

    StructField("overview", StringType(), nullable=True),
    
    # StructField("popularity", DoubleType(),nullable=True),#10 *******
    # StructField("poster_path", StringType(), nullable=True), #*******
    StructField("production_companies", StringType(), nullable =True), 
    # StructField("production_countries", StringType(), nullable=True), # **********
    
    StructField("release_date", StringType(), nullable=True),#14
    
    # StructField("revenue", DoubleType(), nullable=True), #***********
    # StructField("runtime", DoubleType(),nullable=True),#16 ******
    
    StructField("spoken_languages", StringType(), nullable=True),
    
    # StructField("status", StringType(), nullable=True),#18 *******
    
    StructField("tagline", StringType(), nullable=True),
    
    StructField("title", StringType(), nullable=True),#20
    
    # StructField("video", BooleanType(), nullable=True), #*********
    # StructField("vote_average", DoubleType(), nullable=True),#22 ********
    # StructField("vote_count", DoubleType(), nullable=True) #*********
])


def money_formatting(m: str):
    # Remove currency symbols, letters, and whitespaces
    cleaned = re.sub(r'[^\d.,]', '', m)

    # Determine format and remove thousand separators
    if cleaned.count('.') > 1:
        # European format using dots as thousand separators: "1.200.000"
        cleaned = cleaned.replace('.', '')
    elif cleaned.count(',') > 1:
        # Format using commas as thousand separators: "1,200,000"
        cleaned = cleaned.replace(',', '')
    elif ',' in cleaned and '.' in cleaned:
        # Mixed format like "1,000,000.00" — comma is thousand sep, dot is decimal
        cleaned = cleaned.replace(',', '')
    elif cleaned.endswith(',00') or cleaned.endswith(',0'):
        # European decimal comma with no other separators: "1200,00"
        cleaned = cleaned.rsplit(',', 1)[0]
    else:
        # Remove any remaining commas or dots used as decimal separators
        cleaned = re.sub(r'[,.].*$', '', cleaned) if ',' in cleaned or '.' in cleaned else cleaned

    # Convert to float first to handle any remaining decimals, then to int
    return float(cleaned) if cleaned else 0


def parse_line(line: str):
    try:
        reader = csv.reader(
            io.StringIO(line),
            quotechar='"',
            skipinitialspace=True
        )
        row = next(reader)
        # check for correct number of fields per row
        if len(row) < 24:
            return None
        
        if row[0].upper() == "TRUE":
            # still need to implement the rest of the type checking
            return (
                    # adult
                    True,
                    # collection
                    row[1], 
                    # budget
                    #money_formatting(row[2]),
                    # genres
                    row[3],
                    # homepage
                    #row[4],
                    # id (Primary Key)
                    int(row[5].strip()),
                    # imbd id
                    #row[6],
                    # original language
                    #row[7],
                    # original title
                    #row[8],
                    # overview
                    row[9],
                    # popularity
                    #float(row[10]),
                    # poster_path
                    #row[11],
                    # production companies
                    row[12],
                    # production countries
                    #row[13],
                    # release date
                    # TO DO: Fix date formatting ###
                    row[14],
                    # revenue
                    # money_formatting(row[15]),
                    # runtime
                    #float(row[16]),
                    # spoken languages
                    row[17],
                    # status
                    #row[18],
                    # tagline
                    row[19],
                    # title
                    row[20],
                    # video
                    #row[21],
                    # vote average
                    #float(row[22]),
                    # vote count
                    #float(row[23])
                )
        
        else:

            # still need to implement the rest of the type checking
            return (
                    # adult
                    False,
                    # collection
                    row[1], 
                    # budget
                    #money_formatting(row[2]),
                    # genres
                    row[3],
                    # homepage
                    #row[4],
                    # id (Primary Key)
                    int(row[5].strip()),
                    # imbd id
                    #row[6],
                    # original language
                    #row[7],
                    # original title
                    #row[8],
                    # overview
                    row[9],
                    # popularity
                    #float(row[10]),
                    # poster_path
                    #row[11],
                    # production companies
                    row[12],
                    # production countries
                    #row[13],
                    # release date
                    # TO DO: Fix date formatting ###
                    row[14],
                    # revenue
                    # money_formatting(row[15]),
                    # runtime
                    #float(row[16]),
                    # spoken languages
                    row[17],
                    # status
                    #row[18],
                    # tagline
                    row[19],
                    # title
                    row[20],
                    # video
                    #row[21],
                    # vote average
                    #float(row[22]),
                    # vote count
                    #float(row[23])
                )
    except Exception:
        return None







sc = spark.sparkContext

raw = sc.textFile("bronze/movies_metadata.csv")

header = raw.first()
data = raw.filter(lambda line: line != header)

parsed_rdd = (
    data.map(parse_line)
    .filter(lambda x: x != None)
    )

df_meta = spark.createDataFrame(parsed_rdd, meta_schema)

df_meta.show()
df_meta.printSchema()

"""

Gui


Seth




- tagline: keep as string

- id: drop nulls
- title: drop nulls, keep as string

DONE
- adult

"""

#genre
#release_date

@udf(returnType=ArrayType(StringType()))
def parse_genre(s: str):
    if not s:
        return []

    s = s.strip()

    # Strip outer wrapping quote added by CSV reader
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    
    # Extract individual {...} dict strings from the list
    objects = re.findall(r'\{[^{}]*\}', s)

    result = set()
    for obj_str in objects:
        try:
            # Restore placeholder as an escaped single quote so
            # ast.literal_eval sees a valid string literal.
            fixed = ftfy.fix_text(obj_str)
            parsed = json_repair.loads(fixed)
            if isinstance(parsed, dict):
                result.add( parsed.get("name"))
        except Exception as e:
            # Skip this one bad object; keep everything else
            #obj_str = obj_str.replace('\x00', "\\'")
            #print(obj_str)
            continue

    return list(result)


@udf
def parse_date(s: str):
    FORMATS = [
    "%Y-%m-%d",      # 2011-04-25
    "%m/%d/%Y",      # 04/25/1951
    "%m-%d-%Y",      # 11-10-1985
    "%B %d, %Y",     # July 01, 1962
    "%b %d, %Y",     # Jul 01, 1962
    "%d-%m-%Y",      # 25-04-2011
    "%d/%m/%Y",      # 25/04/2011
]
    if not s or not isinstance(s, str):
        return None

    s = s.strip()
    s= s.replace(".", "-")
    s= s.replace("/", "-")
    # strip time portion if present
    if " " in s and not any(c.isalpha() for c in s):
        s = s[:s.find(" ")]

    for fmt in FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            today = dt.now()
            if today < dt:
                continue

            return dt.strftime("%Y-%m-%d")
        except:
            continue

    return None


@udf
def parse_string(s: str):
    if not s:
        return None
    
    s = s.strip()

    try:
        fixed = ftfy.fix_text(s)
        return fixed
    except Exception as e:
        return None

      
      
@udf(returnType=StringType())
def parse_collection(c: str):
    try:
        t = ast.literal_eval(c)
        return t['name']
    except Exception:
        return None

@udf(returnType=ArrayType(StringType()))    
def parse_companies(companies: str):
    try:
        return [item['name'] for item in ast.literal_eval(companies)]
    except Exception:
        return None
@udf(returnType=ArrayType(StringType()))    
def parse_lang(langs: str):
    try:
        return [item['iso_639_1'] for item in ast.literal_eval(langs)]
    except Exception:
        return None


@udf(returnType=BooleanType())
def str_to_bool(s:str):
    s = s.strip()
    s = s.upper()
    mapping = {"TRUE": True, "FALSE": False}
    if mapping.get(s) is not None:
        return mapping[s]
    return None

@udf
def combine_lists(lists):
    result = {x for sublist in lists for x in sublist}
    return list(result)


df_meta_clean = (
    df_meta
    .withColumn("genres", parse_genre(col("genres")))\
    .withColumn("release_date", parse_date(col("release_date")))\
    .withColumn("release_date", to_date(col("release_date")))\
    .withColumn("overview", parse_string(col("overview")))\
    .withColumn("belongs_to_collection",parse_collection(col('belongs_to_collection'))) \
    .withColumn("production_companies", parse_companies(col("production_companies"))) \
    .withColumn("spoken_languages", parse_lang(col("spoken_languages")))\
    .withColumn("tagline", parse_string(col("tagline")))\
)
  
# df_meta_clean.printSchema()
# df_meta_clean.show()

df_genre_merge = df_meta_clean.select("*")\
        .groupBy(col("id"))\
        .agg(
            collect_list("genres").alias("genres"),
            collect_list("spoken_languages").alias("spoken_languages"),
            collect_list("production_companies").alias("production_companies"),
            first("tagline", ignorenulls=True).alias("tagline"),
            first("release_date", ignorenulls=True).alias("release_date"),
            first("overview", ignorenulls=True).alias("overview"),
            first("title", ignorenulls=True).alias("title"),
            first("belongs_to_collection", ignorenulls=True).alias("belongs_to_collection"),
            first("adult", ignorenulls=True).alias("adult")
        )
df_genre_merge = df_genre_merge.withColumn("genres", combine_lists(col("genres")))\
        .withColumn("spoken_languages", combine_lists(col("spoken_languages")))\
        .withColumn("production_companies", combine_lists(col("production_companies")))
df_genre_merge = df_genre_merge.dropna(subset=["id","title"])
df_genre_merge = df_genre_merge.filter(col("title") != "")

df_genre_merge.write.json("ingest/silver/movie_metadata_json", mode="overwrite")
df_genre_merge.write.parquet("ingest/silver/movie_metadata/", mode="overwrite")