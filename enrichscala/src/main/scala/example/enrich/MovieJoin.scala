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
    .join(links, movies("id") === links("tmdbId"), "left")
    .join(ratingsAgg, links("movieId") === ratingsAgg("movieId"), "left")


  }
}