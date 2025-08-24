#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import *


# In[2]:


spark = SparkSession.builder \
    .appName("KafkaConsumerGTFSTU") \
    .master("local[*]") \
    .getOrCreate()


# In[3]:


trip_update_schema = StructType([
    StructField("header", StructType([
        StructField("gtfsRealtimeVersion", StringType()),
        StructField("incrementality", StringType()),
        StructField("timestamp", StringType())
    ])),
    StructField("entity", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("tripUpdate", StructType([
            StructField("trip", StructType([
                StructField("tripId", StringType()),
                StructField("routeId", StringType()),
                StructField("directionId", IntegerType()),
                StructField("startDate", StringType())
            ])),
            StructField("vehicle", StructType([
                StructField("id", StringType())
            ])),
            StructField("stopTimeUpdate", ArrayType(StructType([
                StructField("stopSequence", IntegerType()),
                StructField("stopId", StringType()),
                StructField("arrival", StructType([
                    StructField("time", StringType())
                ])),
                StructField("departure", StructType([
                    StructField("time", StringType())
                ]))
            ]))),
            StructField("timestamp", StringType()),
            StructField("delay", IntegerType())
        ]))
    ])))
])



# In[4]:


raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "gtfs-trip-updates") \
    .option("startingOffsets", "earliest") \
    .load()


# In[5]:


kafka_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str", "topic")


# In[6]:


trip_update_df = kafka_df.select(from_json(col("json_str"), trip_update_schema).alias("data")) \
    .select("data.*")


# In[7]:


trip_update_exploded_df = trip_update_df.select(
    col("header.gtfsRealtimeVersion").alias("gtfs_version"),
    col("header.incrementality").alias("incrementality"),
    col("header.timestamp").alias("header_timestamp"),
    expr("explode(entity) as entity")
).select(
    col("gtfs_version"),
    col("incrementality"),
    col("header_timestamp"),
    col("entity.id").alias("entity_id"),
    col("entity.tripUpdate.trip.tripId").alias("trip_id"),
    col("entity.tripUpdate.trip.routeId").alias("route_id"),
    col("entity.tripUpdate.trip.directionId").alias("direction_id"),
    col("entity.tripUpdate.trip.startDate").alias("start_date"),
    col("entity.tripUpdate.vehicle.id").alias("vehicle_id"),
    expr("explode(entity.tripUpdate.stopTimeUpdate) as stop_update"),
    col("entity.tripUpdate.timestamp").alias("trip_timestamp"),
    col("entity.tripUpdate.delay").alias("delay")
).select(
    col("gtfs_version"),
    col("incrementality"),
    col("header_timestamp"),
    col("entity_id"),
    col("trip_id"),
    col("route_id"),
    col("direction_id"),
    col("start_date"),
    col("vehicle_id"),
    col("stop_update.stopSequence").alias("stop_sequence"),
    col("stop_update.stopId").alias("stop_id"),
    col("stop_update.arrival.time").alias("arrival_time"),
    col("stop_update.departure.time").alias("departure_time"),
    col("trip_timestamp"),
    col("delay"))


# In[8]:


# ClickHouse JDBC connection details
clickhouse_url = "jdbc:clickhouse://clickhouse:8123/gtfs_streaming"
clickhouse_user = "default"
clickhouse_password =  "123"
clickhouse_driver = "com.clickhouse.jdbc.ClickHouseDriver"
clickhouse_table = "trip_updates"

def write_to_clickhouse(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", clickhouse_table) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("driver", clickhouse_driver) \
        .mode("append") \
        .save()

trip_update_query = trip_update_exploded_df.writeStream \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(write_to_clickhouse) \
    .start()




# In[ ]:





# In[ ]:




