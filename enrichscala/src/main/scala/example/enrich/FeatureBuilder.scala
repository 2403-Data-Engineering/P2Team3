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
  .withColumn("character_names", array_remove(col("character_names"),   null.asInstanceOf[String]))
  .withColumn("cast_names",       array_remove(col("cast_names"),       null.asInstanceOf[String]))
  .withColumn("crew_names",       array_remove(col("crew_names"),       null.asInstanceOf[String]))
  .withColumn("directors",        array_remove(col("directors"),        null.asInstanceOf[String]))
  .withColumn("keywords",         array_remove(col("keywords"),         null.asInstanceOf[String]))
  .withColumn("genres",           array_remove(col("genres"),           null.asInstanceOf[String]))
  .withColumn("spoken_languages", array_remove(col("spoken_languages"), null.asInstanceOf[String]))
  .withColumn("production_companies", array_remove(col("production_companies"), null.asInstanceOf[String]))
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