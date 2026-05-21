import weaviate
from weaviate.classes.query import MetadataQuery, Filter

client = weaviate.connect_to_local()
songs = client.collections.get("Movie")

'''response = songs.query.near_text(
    query="songs about love",
    limit=5,
    return_metadata=MetadataQuery(distance=True),
)

print("=== Heartbreak query ===")
for obj in response.objects:
    print(f"  {obj.properties['title']} — {obj.properties['artist']} (distance: {obj.metadata.distance:.3f})")
'''

#query = "Taylor Swift breakup songs"

'''
response = songs.query.near_text(
    query=query,
    limit=5,
    return_metadata=MetadataQuery(distance=True))

for obj in response.objects:
    print(f"  {obj.properties['title']} — {obj.properties['overview']}\n ")

'''

'''
        wc.Property(name="title", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="tagline", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="overview", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="character_names", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="cast_names", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="directors", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="crew_names", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="release_date", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="production_companies", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="adult", data_type=wc.DataType.BOOL, skip_vectorization=True),
        wc.Property(name="avg_rating", data_type=wc.DataType.NUMBER, skip_vectorization=True),
        wc.Property(name="rating_count", data_type=wc.DataType.NUMBER, skip_vectorization=True),
        wc.Property(name="keywords", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="genres", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="spoken_languages", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
        wc.Property(name="belongs_to_collection", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="embedding", data_type=wc.DataType.TEXT),
'''
try:
    while True:
        query = input("Search: ")
        if query.strip() == "":
            continue
        
        response = songs.query.hybrid(query=query, limit=5, alpha=.65)
        for obj in response.objects:
                print(
        f"""
            Title:      {obj.properties['title']}
            Overview:   {obj.properties['overview']}
            Charaters:  {obj.properties['character_names']}
            Actors:     {obj.properties['cast_names']}
            Directors:  {obj.properties['directors']}
            release:    {obj.properties['release_date']}
            production: {obj.properties['production_companies']}
            avg rating: {obj.properties['avg_rating']}
            languages:  {obj.properties['spoken_languages']}
        """)
except:
    pass

client.close()