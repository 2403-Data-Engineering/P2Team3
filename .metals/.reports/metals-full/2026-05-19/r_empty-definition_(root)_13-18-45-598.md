error id: file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/src/main/scala/example/Gold.scala:
file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/src/main/scala/example/Gold.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/example/enrich.
	 -example/enrich.
	 -scala/Predef.example.enrich.
offset: 158
uri: file:///C:/Users/sbucu/p2team3/P2Team3/enrichscala/src/main/scala/example/Gold.scala
text:
```scala
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import exampleenrich.RatingsAgg
import example.enrich@@.MovieJoin
import example.enrich.FeatureBuilder

object Gold {

  def run(spark: SparkSession): Unit = {

    // -----------------------------
    // Load Silver layer datasets
    // -----------------------------
    val movies = spark.read.parquet("data/silver/movies_clean")
    val credits = spark.read.parquet("data/silver/credits_clean")
    val keywords = spark.read.parquet("data/silver/keywords_clean")
    val ratings = spark.read.parquet("data/silver/ratings_clean")

    // -----------------------------
    // Step 1: Aggregate ratings
    // -----------------------------
    val ratingsAgg = RatingsAgg.build(ratings)

    // -----------------------------
    // Step 2: Join all datasets
    // -----------------------------
    val joined = MovieJoin.build(
      movies,
      credits,
      keywords,
      ratingsAgg
    )

    // -----------------------------
    // Step 3: Feature engineering
    // -----------------------------
    val goldDf = FeatureBuilder.build(joined)

    // -----------------------------
    // Optional debug (uncomment if needed)
    // -----------------------------
    // goldDf.show(20, false)
    // goldDf.printSchema()

    // -----------------------------
    // Step 4: Write Gold output
    // -----------------------------
    goldDf.write
      .mode("overwrite")
      .parquet("data/gold/movie_features")

    println("Gold layer completed successfully.")
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 