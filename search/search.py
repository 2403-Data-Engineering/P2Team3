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
query = input("Search: ")

for alpha in [1.0, 0.5, 0.0]:
    print(f"\n=== Hybrid search, alpha={alpha} ===")
    response = songs.query.hybrid(query=query, limit=5, alpha=alpha)
    for obj in response.objects:
        print(f"  {obj.properties['title']} — {obj.properties['artist']}")

client.close()