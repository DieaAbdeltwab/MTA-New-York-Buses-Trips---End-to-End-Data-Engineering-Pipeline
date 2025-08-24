from confluent_kafka import Consumer, KafkaException
import boto3
import time

# ---- Configuration ----
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'minio-writer',
    'auto.offset.reset': 'earliest',
    'fetch.message.max.bytes': 40_000_000,
    'max.partition.fetch.bytes': 40_000_000

}

TOPICS = ['gtfs-trip-updates', 'gtfs-vehicle-positions', 'gtfs-alerts']
BUCKET_NAME = 'gtfsrl-raw'

MINIO_CONFIG = {
    'endpoint_url': 'http://localhost:9002',
    'aws_access_key_id': 'minio',
    'aws_secret_access_key': 'minio123'
}

# ---- Initialize Consumer and S3 ----
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(TOPICS)

s3 = boto3.client('s3', **MINIO_CONFIG)

print("✅ Kafka Consumer started. Waiting for messages...")

# ---- Poll Loop ----
while True:
    try:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        topic = msg.topic()
        value = msg.value()
        timestamp = int(time.time())

        key = f"{topic}/{timestamp}.json"

        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=value)
        print(f"📤 Saved message from {topic} to {key}")

    except Exception as e:
        print(f"❌ Error: {e}")
