package example.enrich

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MovieEnrich {

  def buildMovieFeatures(
      movies: DataFrame,
      credits: DataFrame,
      keywords: DataFrame,
      ratings: DataFrame
  ): DataFrame = {

    // -----------------------------
    // Aggregate ratings
    // -----------------------------
    val ratingsAgg =
      ratings.groupBy("movieId")
        .agg(
          avg("rating").alias("avg_rating"),
          count("rating").alias("rating_count")
        )

    // -----------------------------
    // Join datasets
    // -----------------------------
    val joined =
      movies
        .join(credits, Seq("id"), "left")
        .join(keywords, Seq("id"), "left")
        .join(ratingsAgg,
          movies("id") === ratingsAgg("movieId"),
          "left"
        )

    // -----------------------------
    // Build combined embedding text
    // -----------------------------
    val finalDf =
      joined.withColumn(
        "combined_text",
        concat_ws(
          " ",
          col("title"),
          col("tagline"),
          col("overview"),
          col("top_cast"),
          col("keywords_text"),
          col("genres_text")
        )
      )

    finalDf
  }
}