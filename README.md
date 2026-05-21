# P2Team3
- Christian Duffoo
- Guilherme Vilatoro Taglianeti
- Seth Gleason
- Silas Bucur


## Movie Metadata
We decided to drop some of the columns during the ingestion phase since they were not going to be relevant to the embedding, or were covered by other files (e.g. popularity is covered by ratings). For fields like genres, production companies, and languages, we converted the JSON objects into arrays containing the name only.

We only dropped rows that had the wrong number of columns or null values for either id or title.
### Silver Schema
```
root
 |-- id: integer (nullable = false)
 |-- genres: array (nullable = false)
 |    |-- element: string (containsNull = true)
 |-- spoken_languages: array (nullable = false)
 |    |-- element: string (containsNull = true)
 |-- production_companies: array (nullable = false)
 |    |-- element: string (containsNull = true)
 |-- tagline: string (nullable = true)
 |-- release_date: date (nullable = true)
 |-- overview: string (nullable = true)
 |-- title: string (nullable = true)
 |-- belongs_to_collection: string (nullable = true)
 |-- adult: boolean (nullable = true)
```
