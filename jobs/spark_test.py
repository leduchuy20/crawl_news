from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-test").getOrCreate()
print("SPARK OK")
spark.stop()
