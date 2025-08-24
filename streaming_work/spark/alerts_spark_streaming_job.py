#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import *

# ClickHouse connection details
CLICKHOUSE_HOST = "172.18.0.12"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "123"
CLICKHOUSE_DB = "gtfs_streaming"
CLICKHOUSE_TABLE = "gtfs_alerts"

print(f"✅ ClickHouse table `{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}` is ready.")

# Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumerGTFSA") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for GTFS Realtime JSON
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
# Read from Kafka
try:
    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "gtfs-alerts") \
        .option("startingOffsets", "earliest") \
        .load()
except Exception as e:
    print(f"Failed to read from Kafka: {str(e)}")
    spark.stop()
    exit(1)

kafka_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str", "topic")
alert_df = kafka_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Flatten nested JSON
df_entity = alert_df.select("header", explode("entity").alias("entity"))

df_active_period = df_entity.select(
    col("header.gtfsRealtimeVersion").alias("gtfsRealtimeVersion"),
    col("header.timestamp").alias("timestamp"),
    col("entity.id").alias("id"),
    explode("entity.alert.activePeriod").alias("activePeriod"),
    col("entity.alert.informedEntity").alias("informedEntity"),
    col("entity.alert.headerText").alias("headerText"),
    col("entity.alert.descriptionText").alias("descriptionText")
).filter(col("activePeriod").isNotNull())

df_informed_entity = df_active_period.select(
    "gtfsRealtimeVersion",
    "timestamp",
    "id",
    col("activePeriod.start").alias("activePeriod_start"),
    col("activePeriod.end").alias("activePeriod_end"),
    explode("informedEntity").alias("informedEntity"),
    "headerText",
    "descriptionText"
).filter(col("informedEntity").isNotNull())

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
).filter(col("headerTranslation").isNotNull())

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
).filter(col("descriptionTranslation").isNotNull())

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

# JDBC details for Spark
clickhouse_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
clickhouse_properties = {
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASS,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# Function to write each micro-batch to ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    try:
        batch_df.write \
            .mode("append") \
            .jdbc(clickhouse_url, CLICKHOUSE_TABLE, properties=clickhouse_properties)
        print(f"Batch {batch_id} written successfully to ClickHouse")
    except Exception as e:
        print(f"Error writing batch {batch_id} to ClickHouse: {str(e)}")

# Start streaming to ClickHouse with a persistent checkpoint directory
try:
    alert_query = df_final.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", "/home/rabie/spark-checkpoints/alerts") \
        .trigger(processingTime="30 seconds") \
        .start()
    alert_query.awaitTermination()
except Exception as e:
    print(f"Streaming query failed: {str(e)}")
finally:
    spark.stop()