#!/usr/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.streaming import StreamingQuery
import uuid
import json
import pymongo
import time

# define schema
schema = StructType([
    StructField("device", StringType(), True),
    StructField("bandi_market", DoubleType(), True),
    StructField("city_market", DoubleType(), True),
    StructField("grocery_shop", DoubleType(), True)])

# create spark session
spark = SparkSession \
    .builder \
    .appName("Stream Handler") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/store.crop") \
    .getOrCreate()

# read from Kafka
inputDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crop") \
    .load()

# only select 'value' from the table,
# convert from bytes to string
rawDF = inputDF \
    .selectExpr("CAST(value AS STRING)") \
    .select(col("value").cast("string"))

# split each row on comma, load it to the dataframe
expandedDF = rawDF \
    .selectExpr("split(value, ',')[1] as device", "split(value, ',')[2] as bandi_market", \
                "split(value, ',')[3] as city_market", "split(value, ',')[4] as grocery_shop") \
    .selectExpr("device", "cast(bandi_market as double) as bandi_market", \
                "cast(city_market as double) as city_market", "cast(grocery_shop as double) as grocery_shop")

# groupby and aggregate
summaryDf = expandedDF \
    .groupBy("device") \
    .agg(avg("bandi_market"), avg("city_market"), avg("grocery_shop"))

# create a user-defined function that creates UUIDs
def makeUUID():
    return str(uuid.uuid1())

# register the UDF
make_uuid_udf = udf(lambda: makeUUID())

# add the UUIDs and renamed the columns
# this is necessary so that the dataframe matches the 
# collection schema in MongoDB
summaryWithIDs = summaryDf \
    .withColumn("uuid", make_uuid_udf()) \
    .withColumnRenamed("avg(bandi_market)", "bandi_market") \
    .withColumnRenamed("avg(city_market)", "city_market") \
    .withColumnRenamed("avg(grocery_shop)", "grocery_shop") 

# define a function to save data to MongoDB and measure execution time
def save_to_mongodb(df, epoch_id):
    start_time = time.time()
    
    # convert dataframe to list of dictionaries
    docs = df.toJSON().map(lambda j: json.loads(j)).collect()
    
    # insert documents into MongoDB collection
    if len(docs) > 0:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["store"]
        collection = db["crop"]
        collection.insert_many(docs)
    
    end_time = time.time()
    print(f"Task {epoch_id} took {end_time - start_time:.2f} seconds to execute.")

# write dataframe to MongoDB
query = summaryWithIDs \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(save_to_mongodb) \
    .start()

query.awaitTermination()

