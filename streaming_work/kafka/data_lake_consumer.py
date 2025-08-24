from confluent_kafka import Consumer, KafkaException
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
import time
from datetime import datetime

# ---- Kafka Configuration ----
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'adls-writer',
    'auto.offset.reset': 'earliest',
    'fetch.message.max.bytes': 40_000_000,
    'max.partition.fetch.bytes': 40_000_000
}

TOPICS = ['gtfs-trip-updates', 'gtfs-vehicle-positions', 'gtfs-alerts']

# ---- Azure ADLS Configuration ----
TENANT_ID = ''
CLIENT_ID = ''
CLIENT_SECRET = ''
STORAGE_ACCOUNT_NAME = 'gtfsdls'
CONTAINER_NAME = 'gtfsrl-raw'

# ---- Authenticate using Service Principal ----
credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

blob_service_client = BlobServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    credential=credential
)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# ---- Initialize Kafka Consumer ----
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(TOPICS)

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
        day = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        blob_name = f"{topic}/{day}/{timestamp}.json"

        # Upload to ADLS Gen2 (Blob Storage)
        container_client.upload_blob(name=blob_name, data=value)
        print(f"📤 Saved message from {topic} to {blob_name}")

    except Exception as e:
        print(f"❌ Error: {e}")
