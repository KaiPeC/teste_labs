from pyspark.sql import SparkSession
import pyspark.sql.types as st
from pyspark.sql.functions import *
from utilis import spark_test_labs

df = spark_test_labs.read_parquet(zone="raw",path="yelp_academic_dataset_checkin")


url = "jdbc:mysql://177.93.135.139:3306/teste_labs"   

name_table = "yelp_academic_dataset_checkin"
senha = "1998kai@"
usuario = "teste_labs"

spark_test_labs.save_to_mysql(df,
                              url,
                              table = name_table,
                              mode = "overwrite",
                              user= usuario,
                              password= senha,
                              particoes = 200)



