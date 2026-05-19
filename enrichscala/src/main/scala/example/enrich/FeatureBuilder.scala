package example.enrich

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FeatureBuilder {

  def build(df: DataFrame): DataFrame = {

    df.withColumn(
      "combined_text",
      concat_ws(
        " ",
        col("title"),
        col("overview"),
        col("tagline"),
        col("keywords_text"),
        col("cast_text"),
        col("genres_text")
      )
    )
  }
}