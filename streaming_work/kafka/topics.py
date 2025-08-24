from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

topics = [
    NewTopic("gtfs-trip-updates", num_partitions=3, replication_factor=1),
    NewTopic("gtfs-vehicle-positions", num_partitions=2, replication_factor=1),
    NewTopic("gtfs-alerts", num_partitions=1, replication_factor=1),
]

fs = admin_client.create_topics(topics)
for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
