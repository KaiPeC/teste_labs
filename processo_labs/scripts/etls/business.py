from pyspark.sql.functions import *
from utilis import spark_test_labs

df = spark_test_labs.read_json(zone="transient",path="yelp_academic_dataset_business")

df_select = df.select(
    col("address"),
    col("attributes.AcceptsInsurance").alias("AcceptsInsurance"),
    col("attributes.AgesAllowed").alias("AgesAllowed"),
    col("attributes.Alcohol").alias("Alcohol"),
    col("attributes.Ambience").alias("Ambience"),
    col("attributes.BYOB").alias("BYOB"),
    col("attributes.BYOBCorkage").alias("BYOBCorkage"),
    col("attributes.BestNights").alias("BestNights"),
    col("attributes.BikeParking").alias("BikeParking"),
    col("attributes.BusinessAcceptsBitcoin").alias("BusinessAcceptsBitcoin"),
    col("attributes.BusinessAcceptsCreditCards").alias("BusinessAcceptsCreditCards"),
    col("attributes.BusinessParking").alias("BusinessParking"),
    col("attributes.ByAppointmentOnly").alias("ByAppointmentOnly"),
    col("attributes.Caters").alias("Caters"),
    col("attributes.CoatCheck").alias("CoatCheck"),
    col("attributes.Corkage").alias("Corkage"),
    col("attributes.DietaryRestrictions").alias("DietaryRestrictions"),
    col("attributes.DogsAllowed").alias("DogsAllowed"),
    col("attributes.DriveThru").alias("DriveThru"),
    col("attributes.GoodForDancing").alias("GoodForDancing"),
    col("attributes.GoodForKids").alias("GoodForKids"),
    col("attributes.GoodForMeal").alias("GoodForMeal"),
    col("attributes.HairSpecializesIn").alias("HairSpecializesIn"),
    col("attributes.HappyHour").alias("HappyHour"),
    col("attributes.HasTV").alias("HasTV"),
    col("attributes.Music").alias("Music"),
    col("attributes.NoiseLevel").alias("NoiseLevel"),
    col("attributes.Open24Hours").alias("Open24Hours"),
    col("attributes.OutdoorSeating").alias("OutdoorSeating"),
    col("attributes.RestaurantsAttire").alias("RestaurantsAttire"),
    col("attributes.RestaurantsCounterService").alias("RestaurantsCounterService"),
    col("attributes.RestaurantsDelivery").alias("RestaurantsDelivery"),
    col("attributes.RestaurantsGoodForGroups").alias("RestaurantsGoodForGroups"),
    col("attributes.RestaurantsPriceRange2").alias("RestaurantsPriceRange2"),
    col("attributes.RestaurantsReservations").alias("RestaurantsReservations"),
    col("attributes.RestaurantsTableService").alias("RestaurantsTableService"),
    col("attributes.RestaurantsTakeOut").alias("RestaurantsTakeOut"),
    col("attributes.Smoking").alias("Smoking"),
    col("attributes.WheelchairAccessible").alias("WheelchairAccessible"),
    col("attributes.WiFi").alias("WiFi"),
    col("business_id"),
    col("categories"),
    col("city"),
    col("hours.Friday").alias("Friday"),
    col("hours.Monday").alias("Monday"),
    col("hours.Saturday").alias("Saturday"),
    col("hours.Sunday").alias("Sunday"),
    col("hours.Thursday").alias("Thursday"),
    col("hours.Tuesday").alias("Tuesday"),
    col("hours.Wednesday").alias("Wednesday"),
    col("is_open"),
    col("latitude"),
    col("longitude"),
    col("name"),
    col("postal_code"),
    col("review_count"),
    col("stars"),
    col("state")
)
spark_test_labs.write_parquet(df_select,mode="overwrite",zone="raw",path="yelp_academic_dataset_business")       



