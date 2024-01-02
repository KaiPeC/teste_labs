from pyspark.sql import SparkSession
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from utilis import spark_test_labs


schema = ""

df = spark_test_labs.read_json(zone="transient",path="yelp_academic_dataset_checkin")

spark_test_labs.write_parquet(df,mode="overwrite",zone="raw",path="yelp_academic_dataset_checkin") 
