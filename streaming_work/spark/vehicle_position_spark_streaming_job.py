#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import *



# In[2]:


spark = SparkSession.builder \
    .appName("KafkaConsumerGTFSVP") \
    .master("local[*]") \
    .getOrCreate()


# In[3]:


# client = clickhouse_connect.get_client(host='clickhouse', port=9000, username='default', password='123')

# client.command("CREATE DATABASE IF NOT EXISTS gtfs_streaming")

# client.command("""
# CREATE TABLE IF NOT EXISTS gtfs_streaming.vehicle_positions (
#     gtfs_version String,
#     header_timestamp String,
#     entity_id String,
#     trip_id String,
#     route_id String,
#     start_date String,
#     latitude Float64,
#     longitude Float64,
#     vehicle_timestamp String
# ) ENGINE = MergeTree()
# ORDER BY (trip_id, entity_id)
# """)


# In[4]:


vehicle_schema = StructType([
    StructField("header", StructType([
        StructField("gtfsRealtimeVersion", StringType()),
        StructField("timestamp", StringType())
    ])),
    StructField("entity", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("vehicle", StructType([
            StructField("trip", StructType([
                StructField("tripId", StringType()),
                StructField("routeId", StringType()),
                StructField("startDate", StringType())
            ])),
            StructField("position", StructType([
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType())
            ])),
            StructField("timestamp", StringType())
        ]))
    ])))
])


# In[5]:


raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "gtfs-vehicle-positions") \
    .option("startingOffsets", "earliest") \
    .load()


# In[6]:


kafka_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str", "topic")


# In[7]:


vehicle_df = kafka_df.select(from_json(col("json_str"), vehicle_schema).alias("data")) \
    .select("data.*")


# In[8]:


vehicle_exploded_df = vehicle_df.select(
    col("header.gtfsRealtimeVersion").alias("gtfs_version"),
    col("header.timestamp").alias("header_timestamp"),
    expr("explode(entity) as entity")
).select(
    col("gtfs_version"),
    col("header_timestamp"),
    col("entity.id").alias("entity_id"),
    col("entity.vehicle.trip.tripId").alias("trip_id"),
    col("entity.vehicle.trip.routeId").alias("route_id"),
    col("entity.vehicle.trip.startDate").alias("start_date"),
    col("entity.vehicle.position.latitude").alias("latitude"),
    col("entity.vehicle.position.longitude").alias("longitude"),
    col("entity.vehicle.timestamp").alias("vehicle_timestamp")
)


# In[9]:


clickhouse_url = "jdbc:clickhouse://clickhouse:8123/gtfs_streaming"
clickhouse_properties = {
    "user": "default",
    "password": "123",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


# In[10]:


vehicle_query = vehicle_exploded_df.writeStream \
    .foreachBatch(lambda df, _: df.write.jdbc(
        url=clickhouse_url,
        table="vehicle_positions",
        mode="append",
        properties=clickhouse_properties
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/cp_vehicle_positions") \
    .trigger(processingTime="30 seconds") \
    .start()





# In[ ]:





# In[ ]:





# In[ ]:




