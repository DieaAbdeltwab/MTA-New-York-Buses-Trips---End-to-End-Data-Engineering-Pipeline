#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import *


# In[ ]:


spark = SparkSession.builder \
    .appName("KafkaConsumerGTFSVP") \
    .master("local[*]") \
    .getOrCreate()


# In[3]:


# 🚐 Vehicle Positions Schema
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


# In[4]:


# Read from Kafka (subscribe to all three topics)
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "gtfs-vehicle-positions") \
    .option("startingOffsets", "earliest") \
    .load()


# In[5]:


kafka_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str", "topic")


# In[6]:


vehicle_df = kafka_df.select(from_json(col("json_str"), vehicle_schema).alias("data")) \
    .select("data.*")


# In[7]:


# Flatten the entity array
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


# In[ ]:


# Write stream to console
vehicle_query = vehicle_exploded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", True) \
    .trigger(processingTime="30 seconds") \
    .start()


# In[12]:


vehicle_query.stop()


# In[ ]:


alert_exploded_df = alert_df.select(
    col("header.gtfsRealtimeVersion").alias("gtfs_version"),
    col("header.timestamp").alias("header_timestamp"),
    expr("explode(entity) as entity")
).select(
    col("gtfs_version"),
    col("header_timestamp"),
    col("entity.id").alias("entity_id"),
    expr("explode(entity.alert.activePeriod) as active_period"),
    expr("explode(entity.alert.informedEntity) as informed_entity"),
    expr("explode(entity.alert.headerText.translation) as header_translation"),
    expr("explode(entity.alert.descriptionText.translation) as description_translation")
).select(
    col("gtfs_version"),
    col("header_timestamp"),
    col("entity_id"),
    col("active_period.start").alias("active_period_start"),
    col("active_period.end").alias("active_period_end"),
    col("informed_entity.agencyId").alias("agency_id"),
    col("informed_entity.routeId").alias("route_id"),
    col("header_translation.text").alias("header_text"),
    col("header_translation.language").alias("header_language"),
    col("description_translation.text").alias("description_text"),
    col("description_translation.language").alias("description_language")
)


# In[ ]:


alert_query = alert_exploded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", True) \
    .option("numRows", 10) \
    .option("checkpointLocation", "/path/to/alert_checkpoint") \
    .start()


# In[43]:


alert_query.stop()


# In[ ]:




