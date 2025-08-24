#!/usr/bin/env python
# coding: utf-8

# In[27]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, explode
from pyspark.sql.types import *


# In[2]:


spark = SparkSession.builder \
    .appName("KafkaConsumerGTFSVP") \
    .master("local[*]") \
    .getOrCreate()


# In[30]:


# Define the schema for the GTFS Realtime JSON
schema = StructType([
    StructField("header", StructType([
        StructField("gtfsRealtimeVersion", StringType(), True),
        StructField("timestamp", StringType(), True)
    ]), True),
    StructField("entity", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("alert", StructType([
            StructField("activePeriod", ArrayType(StructType([
                StructField("start", StringType(), True),
                StructField("end", StringType(), True)
            ]), True), True),
            StructField("informedEntity", ArrayType(StructType([
                StructField("agencyId", StringType(), True),
                StructField("routeId", StringType(), True)
            ]), True), True),
            StructField("headerText", StructType([
                StructField("translation", ArrayType(StructType([
                    StructField("text", StringType(), True),
                    StructField("language", StringType(), True)
                ]), True), True)
            ]), True),
            StructField("descriptionText", StructType([
                StructField("translation", ArrayType(StructType([
                    StructField("text", StringType(), True),
                    StructField("language", StringType(), True)
                ]), True), True)
            ]), True)
        ]), True)
    ]), True), True)
])


# In[29]:


df_entity.printSchema()


# In[13]:


# Read from Kafka (subscribe to all three topics)
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "gtfs-alerts") \
    .option("startingOffsets", "earliest") \
    .load()


# In[14]:


kafka_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str", "topic")


# In[15]:


alert_df = kafka_df.select(from_json(col("json_str"), alert_schema).alias("data")) \
    .select("data.*")


# In[33]:


# Explode the entity array
df_entity = alert_df.select("header", explode("entity").alias("entity"))

# Explode the activePeriod array
df_active_period = df_entity.select(
    col("header.gtfsRealtimeVersion").alias("gtfsRealtimeVersion"),
    col("header.timestamp").alias("timestamp"),
    col("entity.id").alias("id"),
    explode("entity.alert.activePeriod").alias("activePeriod"),
    col("entity.alert.informedEntity").alias("informedEntity"),
    col("entity.alert.headerText").alias("headerText"),
    col("entity.alert.descriptionText").alias("descriptionText")
).filter(col("activePeriod").isNotNull())  # Filter out null activePeriod rows

# Explode the informedEntity array
df_informed_entity = df_active_period.select(
    "gtfsRealtimeVersion",
    "timestamp",
    "id",
    col("activePeriod.start").alias("activePeriod_start"),
    col("activePeriod.end").alias("activePeriod_end"),
    explode("informedEntity").alias("informedEntity"),
    "headerText",
    "descriptionText"
).filter(col("informedEntity").isNotNull())  # Filter out null informedEntity rows

# Explode the headerText.translation array
df_header_text = df_informed_entity.select(
    "gtfsRealtimeVersion",
    "timestamp",
    "id",
    "activePeriod_start",
    "activePeriod_end",
    col("informedEntity.agencyId").alias("agencyId"),
    col("informedEntity.routeId").alias("routeId"),
    explode("headerText.translation").alias("headerTranslation"),
    "descriptionText"
).filter(col("headerTranslation").isNotNull())  # Filter out null headerTranslation rows

# Explode the descriptionText.translation array
df_final = df_header_text.select(
    "gtfsRealtimeVersion",
    "timestamp",
    "id",
    "activePeriod_start",
    "activePeriod_end",
    "agencyId",
    "routeId",
    col("headerTranslation.text").alias("header_text"),
    col("headerTranslation.language").alias("header_language"),
    explode("descriptionText.translation").alias("descriptionTranslation")
).filter(col("descriptionTranslation").isNotNull())  # Filter out null descriptionTranslation rows

# Final flattened DataFrame
df_final = df_final.select(
    "gtfsRealtimeVersion",
    "timestamp",
    "id",
    "activePeriod_start",
    "activePeriod_end",
    "agencyId",
    "routeId",
    "header_text",
    "header_language",
    col("descriptionTranslation.text").alias("description_text"),
    col("descriptionTranslation.language").alias("description_language")
)


# In[34]:


alert_query = df_final.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", True) \
    .trigger(processingTime="30 seconds") \
    .start()


# In[35]:


alert_query.stop()


# In[ ]:




