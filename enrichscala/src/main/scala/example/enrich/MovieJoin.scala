package example.enrich

import org.apache.spark.sql.DataFrame

object MovieJoin {

  def build(
      movies: DataFrame,
      credits: DataFrame,
      keywords: DataFrame,
      ratingsAgg: DataFrame
  ): DataFrame = {

    movies
      .join(credits, Seq("id"), "left")
      .join(keywords, Seq("id"), "left")
      .join(ratingsAgg, movies("id") === ratingsAgg("movieId"), "left")
  }
}