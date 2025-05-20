from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, split, explode, trim

spark = SparkSession.builder \
    .appName("JSON to Hive") \
    .enableHiveSupport() \
    .getOrCreate()

df=spark.read.json("hdfs:///user/maria_dev/yelp/yelp_academic_dataset_business.json")
def flatten(schema,prefix=None):
    fields=[]
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype=field.dataType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)
    return fields 
df.write.mode("overwrite").saveAsTable("yelp_business")

df_biz=df.select(flatten(df.schema))
df_biz.write.mode("overwrite").saveAsTable("yelp_business")

#df_categories = df.select("business_id", explode(split(col("categories"), ",")).alias("category")).withColumn("category", trim(col("category")))

#df_categories.write.mode("overwrite").saveAsTable("yelp_business_category")

df = df.drop("category", "attributes", "hours")
