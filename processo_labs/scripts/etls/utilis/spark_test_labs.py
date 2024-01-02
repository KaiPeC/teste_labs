from pyspark.sql import SparkSession
import os
cwd = os.getcwd()

spark = SparkSession.builder \
        .appName("kaka") \
        .master("local[2]") \
        .config("spark.driver.host", "localhost")\
        .config("spark.driver.extraClassPath", "mysql-connector-j-8.2.0.jar")\
        .getOrCreate()

def read_json(zone,path,schema=""): 
        if schema == "":
                return spark.read.json(f"{cwd}/processo_labs/Bucket/{zone}/{path}.json")
        else:
                return spark.read.schema(schema).json(f"{cwd}/processo_labs/Bucket/{zone}/{path}.json")

def read_parquet(zone,path): 
        return spark.read.parquet(f"{cwd}/processo_labs/Bucket/{zone}/{path}")
def write_parquet(df,zone,mode,path):
    return df.write.format("parquet").mode(mode).save(f"{cwd}/processo_labs/Bucket/{zone}/{path}")

def save_to_mysql(df,url,table,mode,user,password,particoes=200):


    df.repartition(particoes).write.jdbc(url=url+
                  f"?user={user}&password={password}&rewriteBatchedStatements=true",
              table=table,
              mode=mode,
              properties={"driver": 'com.mysql.cj.jdbc.Driver'})