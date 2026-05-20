import weaviate
from weaviate.classes.query import MetadataQuery, Filter

client = weaviate.connect_to_local()
movies = client.collections.get("Movie")

query = input("Search: ")

response = movies.query.hybrid(
    query=query,
    limit=5,
    alpha=0.5,
)

for obj in response.objects:
    print(f"{obj.properties['title']} - distance: {obj.metadata.distance:.3f}")


client.close()