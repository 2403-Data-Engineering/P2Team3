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

## Credits - Gui
 We decided to keep most of the data in the bronze step, as we did not know what to expect later in development, when we moved on to the silver layer we decided to drop many columns that would not add a lot of meaning. 
 
 In terms of duplicates handling, we decided to merge the name, character, and crew name columns, as a duplicate row could have data that the other rows did not have.
 
 We only dropped rows that had the wrong number of columns, empty cast and crew arrays, or null values for id.
### Silver Schema
```
root                                                                            
 |-- id: integer (nullable = true)
 |-- crew: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- department: string (nullable = true)
 |    |    |-- job: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- profile_path: string (nullable = true)
 |    |    |-- gender: integer (nullable = true)
 |    |    |-- person_id: integer (nullable = true)
 |    |    |-- credit_id: string (nullable = true)
 |-- cast: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- cast_id: integer (nullable = true)
 |    |    |-- character: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- profile_path: string (nullable = true)
 |    |    |-- gender: integer (nullable = true)
 |    |    |-- person_id: integer (nullable = true)
 |    |    |-- order: integer (nullable = true)
 |    |    |-- credit_id: string (nullable = true)
```
### Gold Schema
```
root                                                                            
 |-- id: integer (nullable = true)
 |-- cast_names: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- character_names: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- directors: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- crew_names: array (nullable = true)
 |    |-- element: string (containsNull = true)
```


# Keywords
- Keywords.csv file only had columns for id (joins to metadata) and stringified json list of keywords, so no columns were removed
#### Formatting issue in reading csv:
Some rows in keywords.csv had two double-quotes around certain words (""like this"") which structurally breaks that row when trying to read it into spark. We had to physically edit the csv file to remove this issue, which was not ideal.
#### Single quote conversion:
Parsing a json-like string in spark requires double quotes surrounding each attribute and value, whereas keywords.csv uses single-quote. We had to use regex replacement to replace all instances of single quote without affecting values that have an actual apostrophe (it's row would break otherwise).
#### Bracket corruption:
Some rows in the csv file start by immediately listing json objects {} separated by commas instead of being in a list, we used regex to add brackets to the beginning and end of a string if they didn't exist prior.
#### Handling duplicates and nulls:
While there weren't exactly "null" values, there were empty keyword lists []; these rows were dropped since they would not provide any value when joined with metadata. To handle exact duplicates, we used .distinct() on the dataframe before writing to parquet. For keywords specifically, we did not consider near-duplicate values since it's alright for multiple movies to have similar keywords.
#### /xa0 issues:
/xao is an ASCII non-breaking space character in extended ASCII. It doesn't cause issues with reading into a spark string column, but causes null values when parsed, so we used regex replace to remove these artifacts.
### Incomplete Json:
We decided to simply drop rows with incomplete json objects due to time constraints, if we had more time we would have applied advanced json-repairing methods to all of the ingestion phase.
