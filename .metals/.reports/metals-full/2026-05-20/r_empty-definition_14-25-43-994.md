error id: file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/FeatureBuilder.scala:
file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/FeatureBuilder.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/array_join.
	 -org/apache/spark/sql/functions/array_join#
	 -org/apache/spark/sql/functions/array_join().
	 -array_join.
	 -array_join#
	 -array_join().
	 -scala/Predef.array_join.
	 -scala/Predef.array_join#
	 -scala/Predef.array_join().
offset: 857
uri: file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/FeatureBuilder.scala
text:
```scala
package example.enrich

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FeatureBuilder {

  def build(df: DataFrame): DataFrame = {
    
    df
  .withColumn("cast_names", expr("transform(array_sort(cast, (x, y) -> x.order - y.order), x -> x.name)"))
  .withColumn("character_names", expr("transform(array_sort(cast, (x, y) -> x.order - y.order), x -> x.character)"))
  .withColumn("directors", expr("transform(filter(crew, x -> x.department == 'Directing' AND x.job == 'Director'), x -> x.name)"))
  .withColumn("crew_names", expr("transform(filter(crew, x -> NOT (x.department == 'Directing' AND x.job == 'Director')), x -> x.name)"))
  .drop("crew", "cast")
  .withColumn("embedding", concat_ws(" ", col("title"), col("overview"), col("tagline"), array_join(slice(col("cast_names"), 1, 5), " ") ), arr@@ay_join(col("directors"), " "),  )
  //embedding
//"title characters actors directors tagline overview rating keyword genre collection"
  //production_companies, cast_names, directors, keywords, genre
  

    /* properties:
characters all charactes
actors all actors
directors all crew
title
release date
production company
adult
rating
keyword
genre
spoken_language
collection */
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 