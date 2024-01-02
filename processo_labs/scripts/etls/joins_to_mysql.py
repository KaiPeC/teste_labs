from pyspark.sql.functions import *
from utilis import spark_test_labs

url = "jdbc:mysql://177.93.135.139:3306/teste_labs"   

name_table = "refined_dashboard"
senha = "1998kai@"
usuario = "teste_labs"

df_review = (spark_test_labs.read_parquet(zone="raw",path="yelp_academic_dataset_review")
             .filter(col("date")>= "2021-09-01"))
df_business = (spark_test_labs.read_parquet(zone="raw",path="yelp_academic_dataset_business")
               .select(col("name").alias("business_name"),
                       col("postal_code").alias("business_postal_code"),
                       col("state").alias("business_state"),
                       col("stars").alias("business_stars"),
                       col("review_count").alias("business_review_count"),
                       col("city").alias("business_city"),
                       col("categories").alias("business_categories"),
                       col("WiFi"),
                       col("RestaurantsPriceRange2"),
                       col("business_id").alias("business_id_business")
                       ))
df_tip = (spark_test_labs.read_parquet(zone="raw",path="yelp_academic_dataset_tip")
          .select(col("date").alias("tip_date"),
                  col("business_id").alias("business_id_tip"),
                  col("user_id").alias("user_id_tip"),
                  col("text").alias("tip_text")))
df_user = (spark_test_labs.read_parquet(zone="raw",path="yelp_academic_dataset_user")
           .select(col("average_stars").alias("user_avarege_stars"),
                   col("friends").alias("user_friends"),
                   col("name").alias("user_name"),
                   col("review_count").alias("user_review_count"),
                   col("useful").alias("user_useful"),
                   col("user_id").alias("user_id_user")
                   ))

df_final = (df_review.join(df_business, df_review.business_id == df_business.business_id_business, "left").drop("business_id_business")
            .join(df_user, df_review.user_id == df_user.user_id_user,"left").drop("user_id_user")
            .join(df_tip, (df_review.business_id == df_tip.business_id_tip)&(df_review.user_id == df_tip.user_id_tip)).drop("user_id_tip","business_id_tip"))

spark_test_labs.save_to_mysql(df_final,
                              url,
                              table = name_table,
                              mode = "overwrite",
                              user= usuario,
                              password= senha)