import weaviate
import weaviate.classes.config as wc
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("movie-loader").getOrCreate()

gold_path = "enrichscala/output/parquet/"

#df = spark.read.parquet(gold_path)
#df.printSchema()
'''
value = df.limit(5).collect()[0]['embedding']
print(value)
df.printSchema()
spark.stop()
'''

client = weaviate.connect_to_local()
print("Connected:", client.is_ready())

# Clean slate if collection already exists
if client.collections.exists("Movie"):
    client.collections.delete("Movie")

client.collections.create(
    name="Movie",
    vectorizer_config=wc.Configure.Vectorizer.text2vec_transformers(),
    properties=[
        wc.Property(name="title", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="tagline", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="overview", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="character_names", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="cast_names", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="directors", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="crew_names", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="release_date", data_type=wc.DataType.DATE, skip_vectorization=True),
        wc.Property(name="production_companies", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="adult", data_type=wc.DataType.BOOL, skip_vectorization=True),
        wc.Property(name="avg_rating", data_type=wc.DataType.NUMBER, skip_vectorization=True),
        wc.Property(name="rating_count", data_type=wc.DataType.NUMBER, skip_vectorization=True),
        wc.Property(name="keywords", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="genres", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="spoken_languages", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="belongs_to_collection", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="embedding", data_type=wc.DataType.TEXT),
    ],
)


print("Movie collection created.")


df = spark.read.parquet(gold_path)
print(f"Loaded {df.count()} rows from Parquet")

movies = client.collections.get("Movie")

# Open a batch context. The client buffers add_object calls and sends them
# in batched HTTP requests under the hood. `dynamic` lets the client decide
# batch size based on throughput.
with movies.batch.dynamic() as batch:
    # collect() pulls all rows into the driver as a list. Fine for small data;
    # use df.toLocalIterator() for very large datasets.
    for row in df.collect():
        # Build combined_text from the descriptive fields — this is the only
        # property that gets vectorized.
        """
            combined_text = (
            f"{row.title}\n"
            f"Overview: {row.overview}\n"
            f"Tagline: {row.tagline}"
            f"Characters: {', '.join(row.characters)}"
            f"Actors: {', '.join(row.actors)}"
            f"Directors: {', '.join(row.directors)}"
            f"Keywords: {', '.join(row.keywords)}"
            f"Collection: {row.collection}"
            f"Genres: {', '.join(row.genres)}"
        )
        """

 
        # Add one object to the batch. We send raw properties only — Weaviate
        # calls the transformers container to generate the vector server-side.
        # Batches may flush mid-loop when the buffer fills.
        batch.add_object(properties={
            "title": row.title,            
            "tagline": row.tagline,
            "overview": row.overview,
            "character_names": row.character_names,
            "cast_names": row.cast_names,
            "crew_names": row.crew_names,
            "directors": row.directors,
            "release_date": row.release_date,
            "production_companies": row.production_companies,
            "adult": row.adult,
            "avg_rating": row.avg_rating,
            "rating_count": row.rating_count,
            "keywords": row.keywords,
            "genres": row.genres,
            "spoken_languages": row.spoken_languages,
            "belongs_to_collection": row.belongs_to_collection,
            "embedding": row.embedding
        })
# When the `with` block exits, any remaining buffered objects are flushed.

# Always check for failures — batch errors are silent by default
failed = movies.batch.failed_objects
if failed:
    print(f"WARNING: {len(failed)} objects failed to import")
    for f in failed[:3]:
        print(f)
else:
    print("All movies imported successfully.")

# Confirm the count
result = movies.aggregate.over_all(total_count=True)
print(f"Total movies in collection: {result.total_count}")

client.close()
spark.stop()