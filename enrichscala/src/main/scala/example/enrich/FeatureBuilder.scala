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

  //embedding
  .withColumn("")


  }
}