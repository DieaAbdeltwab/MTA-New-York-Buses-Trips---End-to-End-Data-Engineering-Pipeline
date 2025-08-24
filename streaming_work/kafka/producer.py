from confluent_kafka import Producer
import requests
import time
import json
import logging
import clickhouse_connect 

# --- API KEYS ROTATION ---
API_KEYS = [
    "TAbq6RHF55SdPY7y4XzAxgT8IAKqvZwd",
    "5mraP7kVRRSXaumnqi3TCWCqnHQVa0uW",
    "xYHO55InJk9l6fZhiYydd3grig8wknsz"
]

# Assign a specific API key for each feed
FEEDS = {
    "gtfs-trip-updates": {
        "url": "https://transit.land/api/v2/rest/feeds/f-mta~nyc~rt~bustime/download_latest_rt/trip_updates.json",
        "api_key": API_KEYS[0]
    },
    "gtfs-vehicle-positions": {
        "url": "https://transit.land/api/v2/rest/feeds/f-mta~nyc~rt~bustime/download_latest_rt/vehicle_positions.json",
        "api_key": API_KEYS[1]
    },
    "gtfs-alerts": {
        "url": "https://transit.land/api/v2/rest/feeds/f-mta~nyc~rt~bustime/download_latest_rt/alerts.json",
        "api_key": API_KEYS[2]
    }
}

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'gtfs-producer',
    'message.max.bytes': 40000000
})

# Last seen timestamp per feed
last_timestamps = {feed: None for feed in FEEDS}

# ClickHouse connection
ch_client = clickhouse_connect.get_client(
    host="localhost", port=8123, username="default", password="123"
)

def insert_metric(feed, metric_name, value):
    """Insert a metric into ClickHouse"""
    ch_client.command(f"""
        INSERT INTO gtfs_metrics.feed_metrics (timestamp, feed, metric, value)
        VALUES (now(), '{feed}', '{metric_name}', {value})
    """)

def fetch_and_send(feed_name, feed_info):
    try:
        headers = {"apikey": feed_info["api_key"]}
        response = requests.get(feed_info["url"], headers=headers)

        if response.ok:
            data = response.json()

            # extract timestamp from feed header
            header = data.get("header", {})
            feed_timestamp = header.get("timestamp")

            if not feed_timestamp:
                logging.warning(f"No timestamp in {feed_name}, skipping...")
                return

            feed_timestamp = int(feed_timestamp)

            # check for new updates
            if last_timestamps[feed_name] == feed_timestamp:
                logging.info(f"No new update for {feed_name}, skipping...")
                return

            # --- METRICS ---
            if last_timestamps[feed_name]:
                update_interval = feed_timestamp - last_timestamps[feed_name]
                insert_metric(feed_name, "update_interval_sec", update_interval)

            payload_size = len(json.dumps(data))
            insert_metric(feed_name, "payload_size_bytes", payload_size)

            last_timestamps[feed_name] = feed_timestamp

            # --- KAFKA ---
            producer.produce(feed_name, value=json.dumps(data).encode("utf-8"))
            producer.flush()
            logging.info(f"Sent update to Kafka topic {feed_name} (ts={feed_timestamp})")

        else:
            logging.error(f"Failed to fetch {feed_name} | Status Code: {response.status_code}")

    except Exception as e:
        logging.error(f"Error while processing {feed_name}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    while True:
        for topic, feed_info in FEEDS.items():
            fetch_and_send(topic, feed_info)
        print("#################### ANOTHER BATCH ####################")
        time.sleep(60)
