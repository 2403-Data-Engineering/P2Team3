error id: file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/FeatureBuilder.scala:withColumn.
file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/FeatureBuilder.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions.
	 -org/apache/spark/sql/functions#
	 -org/apache/spark/sql/functions().
	 -spark/implicits.
	 -spark/implicits#
	 -spark/implicits().
	 -scala/Predef.
	 -scala/Predef#
	 -scala/Predef().
offset: 691
uri: file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/FeatureBuilder.scala
text:
```scala
package example.enrich

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FeatureBuilder {

  def build(df: DataFrame): DataFrame = {
    import spark.implicits._
    df.select(movies("id"), col("genres"), col("spoken_languages"), col("production_companies"), col("tagline"),col("release_date"), col("overview"), col("title"), col("cast"), col("crew"), col("avg_rating"), col("rating_count"), col("keywords"))
  .withColumn("cast_names", expr("transform(array_sort(cast, (x, y) -> x.order - y.order), x -> x.name)"))
  .withColumn("character_names", expr("transform(array_sort(cast, (x, y) -> x.order - y.order), x -> x.character)"))
  .withCo@@lumn("directors", expr("transform(filter(crew, x -> x.department == 'Directing' AND x.job == 'Director'), x -> x.name)"))
  .withColumn("crew_names", expr("transform(filter(crew, x -> NOT (x.department == 'Directing' AND x.job == 'Director')), x -> x.name)"))
  .drop("crew", "cast")




  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 