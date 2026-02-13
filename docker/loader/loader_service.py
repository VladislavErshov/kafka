import json
import os
import requests
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def main():
    KAFKA_SERVERS = f"{os.getenv('KAFKA_CONTAINER', 'kafka')}:9092"
    TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'HackerNews-items-raw')

    LIMIT = int(os.getenv('HN_LIMIT', '100'))
    TOP_URL = f"{os.getenv('HN_TOP_URL', 'https://hacker-news.firebaseio.com')}/v0/topstories.json"
    ITEM_BASE_URL = f"{os.getenv('HN_ITEM_URL', 'https://hacker-news.firebaseio.com')}/v0/item/"

    producer = None
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(3, 0, 0),
                request_timeout_ms=10000
            )
            print("Connected to Kafka!")
            break
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying ({i+1}/10)...")
            time.sleep(5)

    if not producer:
        print("Could not connect to Kafka. Exiting.")
        return

    print(f"Fetching top {LIMIT} stories from HN...")
    try:
        top_ids = requests.get(TOP_URL).json()[:LIMIT]
        print(f"Fetched top {LIMIT} stories")
    except Exception as e:
        print(f"Failed to fetch top stories: {e}")
        return

    for item_id in top_ids:
        try:
            item = requests.get(f"{ITEM_BASE_URL}{item_id}.json").json()
            if item and 'title' in item:
                payload = {
                    "id": item.get('id'),
                    "title": item.get('title', ''),
                    "url": item.get('url', ''),
                    "score": item.get('score', 0),
                    "author": item.get('by', 'unknown'),
                    "time": item.get('time', 0)
                }

                producer.send(TOPIC, value=payload)
                print(f"Sent to Kafka: {item.get('id')}")
        except Exception as e:
            print(f"Error fetching item {item_id}: {e}")

    producer.flush()
    print("Ingestion finished.")

if __name__ == "__main__":
    main()
