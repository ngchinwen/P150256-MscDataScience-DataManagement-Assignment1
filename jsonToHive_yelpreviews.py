from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

spark = SparkSession.builder \
    .appName("JSON to Hive") \
    .enableHiveSupport() \
    .getOrCreate()

df_review = spark.read.json("hdfs:///user/maria_dev/yelp/yelp_academic_dataset_review.json")
df_review = df_review.withColumn("text",regexp_replace("text",r'\n',' '))

# Save as Hive table
df_review.write.mode("overwrite").saveAsTable("yelp_review")
