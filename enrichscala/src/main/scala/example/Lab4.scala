package example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lab4 {

  def main(args: Array[String]): Unit = {

    // 1. Create SparkSession
    val spark = SparkSession.builder()
      .appName("SimpleSparkExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 2. Sample data (like your ratings dataset)
    val data = Seq(
      (1, 101, 4.0),
      (1, 102, 5.0),
      (2, 101, 3.0),
      (2, 103, 4.5),
      (3, 101, 2.0)
    )

    val df = data.toDF("userId", "movieId", "rating")

    // 3. Show raw data
    println("Raw Data:")
    df.show()

    // 4. Filter (ratings >= 4)
    val filtered = df.filter(col("rating") >= 4.0)

    println("Filtered Data:")
    filtered.show()

    // 5. Aggregate (average rating per movie)
    val avgRatings = df
      .groupBy("movieId")
      .agg(
        avg("rating").alias("avg_rating"),
        count("*").alias("num_ratings")
      )
      .orderBy(desc("avg_rating"))

    println("Aggregated Data:")
    avgRatings.show()

    // 6. Write output (parquet)
    avgRatings.write
      .mode("overwrite")
      .parquet("output/movie_ratings")

    // 7. Stop Spark
    spark.stop()
  }
}