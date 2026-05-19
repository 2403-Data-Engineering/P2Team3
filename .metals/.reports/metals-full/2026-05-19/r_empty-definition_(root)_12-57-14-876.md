error id: file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/src/main/scala/example/Main.scala:
file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/src/main/scala/example/Main.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb

found definition using fallback; symbol MovieEnrich
offset: 89
uri: file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/src/main/scala/example/Main.scala
text:
```scala
package example


import org.apache.spark.sql.SparkSession
import example.enrich.Movi@@eEnrich

object Gold {

  def run(spark: SparkSession): Unit = {

    // -----------------------------
    // Read silver datasets
    // -----------------------------
    val movies =
      spark.read.parquet("data/silver/movies_clean")

    val credits =
      spark.read.parquet("data/silver/credits_clean")

    val keywords =
      spark.read.parquet("data/silver/keywords_clean")

    val ratings =
      spark.read.parquet("data/silver/ratings_clean")

    // -----------------------------
    // Create enriched gold dataset
    // -----------------------------
    val goldDf =
      MovieEnrich.buildMovieFeatures(
        movies,
        credits,
        keywords,
        ratings
      )

    // -----------------------------
    // Write gold parquet
    // -----------------------------
    goldDf.write
      .mode("overwrite")
      .parquet("data/gold/movie_features")

    println("Gold layer completed.")
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 