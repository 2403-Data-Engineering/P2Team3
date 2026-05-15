import os, sys, csv, io, ast, re, json_repair, ftfy

from pyspark import SparkContext
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = f'{sys.executable}'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'{sys.executable}'


from pyspark.sql.types import IntegerType, StringType, ArrayType, StructField, StructType, MapType, BooleanType, DoubleType, DateType
from pyspark.sql.functions import from_json, col, regexp_replace, udf


spark = SparkSession.builder.appName("MovieMetadata").getOrCreate()

belong_schema = ArrayType(StructType[
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("poster_path", StringType(), nullable= True),
    StructField("backdrop_path", StringType(), nullable=True)
])

genre_schema = ArrayType(StructType[
    StructField("id", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

production_company_schema = ArrayType(StructType[
    StructField("name", StringType(), nullable=True),
    StructField("id", IntegerType(), nullable=True)
])

production_country_schema = ArrayType(StructType[
    StructField("iso_3166_1", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

language_schema = ArrayType(StructType[
    StructField("iso_639_1", StringType(),nullable=True),
    StructField("name", StringType(), nullable=True)
])



meta_schema = ArrayType(StructType[
    StructField("adult", BooleanType(), nullable=True), #0
    StructField("belongs_to_collection", ArrayType(belong_schema), nullable=True),
    StructField("budget", IntegerType(), nullable=True), #2
    StructField("genres", ArrayType(genre_schema), nullable=True),
    StructField("homepage", StringType(), nullable=True),#4
    StructField("id", IntegerType(), nullable=False),
    StructField("imdb_id", StringType(), nullable=True),#6
    StructField("original_language", ArrayType(language_schema), nullable=True),
    StructField("original_title", StringType(),nullable=True), #8
    StructField("overview", StringType(), nullable=True),
    StructField("popularity", DoubleType(),nullable=True),#10
    StructField("poster_path", StringType(), nullable=True),
    StructField("production_companies", ArrayType(production_company_schema), nullable =True),
    StructField("production_countries", ArrayType(production_country_schema), nullable=True),
    StructField("release_date", DateType(), nullable=True),#14
    StructField("revenue", DoubleType(), nullable=True),
    StructField("runtime", DoubleType(),nullable=True),#16
    StructField("spoken_languages", ArrayType(language_schema), nullable=True),
    StructField("status", StringType(), nullable=True),#18
    StructField("tagline", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),#20
    StructField("video", BooleanType(), nullable=True),
    StructField("vote_average", DoubleType(), nullable=True),#22
    StructField("vote_count", DoubleType(), nullable=True)
])
""""
TO DO:
    Corruption Handling:
        - date formatting/impossible dates
        - negative/impossible numeric values
        - Stringified JSON corruption
        - mojibake
    Cleaning Data:
        - null handling
        - duplicate handling
        - type validation


"""


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
        
        # check if boolean fields have boolean values
        elif row[0] not in ['False','True'] or row[21] not in ['False','True']:
            return None
            
        else:
            # still need to implement the rest of the type checking
            return (
                    # adult
                    bool(row[0]),
                    # collection
                    row[1], 
                    # budget
                    money_formatting(row[2]),
                    # genres
                    row[3],
                    # homepage
                    row[4],
                    # id (Primary Key)
                    int(row[5].strip()),
                    # imbd id
                    row[6],
                    # original language
                    row[7],
                    # original title
                    row[8],
                    # overview
                    row[9],
                    # popularity
                    float(row[10]),
                    # poster_path
                    row[11],
                    # production companies
                    row[12],
                    # production countries
                    row[13],
                    # release date
                    # TO DO: Fix date formatting ###
                    row[14],
                    # revenue
                    money_formatting(row[15]),
                    # runtime
                    float(row[16]),
                    # spoken languages
                    row[17],
                    # status
                    row[18],
                    # tagline
                    row[19],
                    # title
                    row[20],
                    # video
                    row[21],
                    # vote average
                    float(row[22]),
                    # vote count
                    float(row[23])
                )
    except Exception:
        return None







sc = spark.sparkContext

raw = sc.textFile("bronze/bronze_data_sample/movies_metadata.csv")

header = raw.first()
data = raw.filter(lambda line: line != header)
