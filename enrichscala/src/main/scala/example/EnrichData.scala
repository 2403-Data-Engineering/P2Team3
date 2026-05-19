package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import enrich.RatingsAgg
import enrich.MovieJoin
import enrich.FeatureBuilder


object EnrichData {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
      .appName("EnrichData")
      .master("local[*]")
      .getOrCreate()

      import spark.implicits._
    // Run Gold pipeline
    run(spark)

    spark.stop()
  }

  def run(spark: SparkSession): Unit = {

    // -----------------------------
    // Load Silver layer datasets
    // -----------------------------
    val movies = spark.read.parquet("../ingest/silver/movie_metadata/")
    val credits = spark.read.parquet("../ingest/silver/credits/")
    val keywords = spark.read.parquet("../ingest/silver/keywords/")
    val ratings = spark.read.parquet("../ingest/silver/ratings/")
    val links = spark.read.parquet("../ingest/silver/links/")

    //movies.printSchema()
    //credits.printSchema()
    //keywords.printSchema()
    //ratings.printSchema()
    //links.printSchema()
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
      links,
      ratingsAgg,
      spark
    )
    joined.printSchema()
    joined.show()

    //working up to this point
    /* 
      create the embeding column and concat all the data
      create the column with cast and crew with only relevant data
      create a column with a keyword list.
     */
    
    
    // -----------------------------
    // Step 3: Feature engineering
    // -----------------------------
    //val goldDf = FeatureBuilder.build(joined)

    // -----------------------------
    // Optional debug (uncomment if needed)
    // -----------------------------
    // goldDf.show(20, false)
    // goldDf.printSchema()

    // -----------------------------
    // Step 4: Write Gold output
    // -----------------------------
    //goldDf.write.mode("overwrite").parquet("gold/movie_features")

    println("Gold layer completed successfully.")
  }
}