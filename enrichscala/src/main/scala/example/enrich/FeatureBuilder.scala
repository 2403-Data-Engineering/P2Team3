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
  .withColumn("embedding", concat_ws(" ", col("title"), 
        col("overview"), 
        col("tagline"), 
        array_join(slice(col("cast_names"), 1, 5), " "), 
        array_join(slice(col("character_names"), 1, 10), " "), 
        array_join(col("directors"), " "), 
        array_join(col("keywords"), " "),   
        col("belongs_to_collection"),
        array_join(col("genres"), " ")
        ) )
  .na.fill("n/a")
  .na.fill(0)
  .na.fill(0.0)
  .withColumn("character_names", filter(col("character_names"), x => x.isNotNull))
  .withColumn("character_names", when(col("character_names").isNull, array(lit(""))).otherwise(col("character_names")))
  .withColumn("cast_names", filter(col("cast_names"), x => x.isNotNull))
  .withColumn("cast_names", when(col("cast_names").isNull, array(lit(""))).otherwise(col("cast_names")))
  .withColumn("keywords",filter(col("keywords"), x => x.isNotNull))
  .withColumn("keywords", when(col("keywords").isNull, array(lit(""))).otherwise(col("keywords")))
  .withColumn("directors", filter(col("directors"), x => x.isNotNull))
  .withColumn("directors", when(col("directors").isNull, array(lit(""))).otherwise(col("directors")))
  .withColumn("crew_names", filter(col("crew_names"), x => x.isNotNull))
  .withColumn("crew_names", when(col("crew_names").isNull, array(lit(""))).otherwise(col("crew_names")))
    .withColumn("spoken_languages", filter(col("spoken_languages"), x => x.isNotNull))
  .withColumn("spoken_languages", when(col("spoken_languages").isNull, array(lit(""))).otherwise(col("spoken_languages")))
    .withColumn("production_companies", filter(col("production_companies"), x => x.isNotNull))
  .withColumn("production_companies", when(col("production_companies").isNull, array(lit(""))).otherwise(col("production_companies")))

  //.withColumn("character_names", when(col("character_names").isNull, array()).otherwise(col("character_names")))

  //embedding
//"title characters actors directors tagline overview rating keyword genre collection"
  //production_companies, cast_names, directors, keywords, genre
  

    /* properties:
characters all charactes
actors all actors
directors all crew
title
release date
production company
adult
rating
keyword
genre
spoken_language
collection */
  }
}