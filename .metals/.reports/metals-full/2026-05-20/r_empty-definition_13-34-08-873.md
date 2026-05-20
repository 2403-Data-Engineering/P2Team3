error id: file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/MovieJoin.scala:withColumn.
file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/MovieJoin.scala
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
offset: 970
uri: file:///D:/Revature/P2Team3/enrichscala/src/main/scala/example/enrich/MovieJoin.scala
text:
```scala
package example.enrich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object MovieJoin {

  def build(
      movies: DataFrame,
      credits: DataFrame,
      keywords: DataFrame,
      links: DataFrame,
      ratingsAgg: DataFrame,
      spark: SparkSession
  ): DataFrame = {
      import spark.implicits._

  
  movies
  .join(credits, movies("id") === credits("id"), "left")
  .join(keywords, movies("id") === keywords("id"), "left")
  .join(ratingsAgg, movies("id") === ratingsAgg("movieId"), "left")
  .select(movies("id"), col("genres"), col("spoken_languages"), col("production_companies"), col("tagline"),col("release_date"), col("overview"), col("title"), col("cast"), col("crew"), col("avg_rating"), col("rating_count"), col("keywords"))
  
  .withColumn("cast_names", expr("transform(array_sort(cast, (x, y) -> x.order - y.order), x -> x.name)"))
  .withCol@@umn("cast_names", expr("transform(array_sort(cast, (x, y) -> x.order - y.order), x -> x.name)"))
  
  /*
    var m = movies
  m=movies.join(credits, movies("id") === credits("id"), "left")
  println("movies with credit")
  m.printSchema()
  m.show()


  m=m.join(keywords, movies("id") === keywords("id"), "left")
  println("movies keywords")
  m.printSchema()
  m.show()
  m=m.join(links, movies("id") === links("tmdbId"), "left")
  println("movies with links")
  m.printSchema()
  m.show()
  m=m.join(ratingsAgg, links("movieId") === ratingsAgg("movieId"), "left")
  println("movies with ratings")
  m.printSchema()
  m.show()
  return m
  */

  
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 