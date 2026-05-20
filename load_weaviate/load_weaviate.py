import weaviate
import weaviate.classes.config as wc
from pyspark.sql import SparkSession


gold_path = "enrichscala/output/"

spark = SparkSession.builder.appName("movie-loader").getOrCreate()

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
        wc.Property(name="characters", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="actors", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="directors", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="release_date", data_type=wc.DataType.DATE),
        wc.Property(name="production_company", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="adult", data_type=wc.DataType.BOOL),
        wc.Property(name="rating", data_type=wc.DataType.NUMBER),
        wc.Property(name="keywords", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="genres", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="spoken_languages", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="collection", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="combined_text", data_type=wc.DataType.TEXT),
    ],
)

print("Movie collection created.")


df = spark.read.parquet(gold_path)
df.cache()
df.show()
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
        '''
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
        '''
 
        # Add one object to the batch. We send raw properties only — Weaviate
        # calls the transformers container to generate the vector server-side.
        # Batches may flush mid-loop when the buffer fills.
        batch.add_object(properties={
            "title": row.title,            
            "tagline": row.tagline,
            "overview": row.overview,
            "characters": row.characters,
            "actors": row.actors,
            "directors": row.directors,
            "release_date": row.release_date,
            "production_company": row.production_company,
            "adult": row.adult,
            "ratings": row.ratings,
            "keywords": row.keywords,
            "genres": row.genres,
            "spoken_languages": row.spoken_languages,
            "collection": row.collection,
            "combined_text": row.combined_text
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