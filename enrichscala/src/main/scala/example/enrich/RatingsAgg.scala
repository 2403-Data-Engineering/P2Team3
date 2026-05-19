package example.enrich

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RatingsAgg {

  def build(ratings: DataFrame): DataFrame = {
    ratings
      .groupBy("movieId")
      .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
      )
  }
}