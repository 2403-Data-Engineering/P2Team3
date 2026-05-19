from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
sc = spark.sparkContext


print("Hadoop version: " + sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion())