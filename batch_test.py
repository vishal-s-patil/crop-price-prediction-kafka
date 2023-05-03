import pymongo
from pyspark.sql import SparkSession
import time

# connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# select database and collection
db = client["store"]
collection = db["crop"]

# read data from MongoDB and exclude the _id field
data = collection.find({}, {"_id": 0})

# create a SparkSession
spark = SparkSession.builder \
    .appName("Batch Processing") \
    .getOrCreate()

# convert data to a DataFrame
df = spark.createDataFrame(list(data))

# calculate number of partitions
num_partitions = df.rdd.getNumPartitions()

# perform batch processing on the data
start_time = time.time()
for i in range(num_partitions):
    partition = df.where(f"spark_partition_id() = {i}")
    result = partition.groupBy("device").avg("bandi_market", "city_market", "grocery_shop")
    result.show()
end_time = time.time()

# calculate and print the execution time
print(f"Execution time: {end_time - start_time:.2f} seconds")

# stop the SparkSession
spark.stop()

# close the MongoDB connection
client.close()





