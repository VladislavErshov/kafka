import json
import os
import sys
from kafka import KafkaConsumer, KafkaProducer
from fastembed import TextEmbedding

KAFKA_HOST = os.getenv('KAFKA_CONTAINER', 'kafka-container')
KAFKA_PORT = os.getenv('EXTERNAL_PORT', '9092')
KAFKA_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"

INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'HackerNews-items-raw')
OUTPUT_TOPIC = os.getenv('KAFKA_VECTORIZED_TOPIC', 'HackerNews-items-vectored')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'HackerNews-vectorizer-group')
MODEL_NAME = os.getenv('EMBEDDING_MODEL_NAME', 'BAAI/bge-small-en-v1.5')

try:
    model = TextEmbedding(model_name=MODEL_NAME)
except Exception as e:
    print(f"Failed to load model {MODEL_NAME}: {e}")
    sys.exit(1)

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    api_version=(3, 0, 0)
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 0, 0)
)

def run():
    print(f"Vectorizer started on {KAFKA_SERVERS}. Model: {MODEL_NAME}")
    for message in consumer:
        item = message.value
        try:
            title = item.get('title')
            if title:
                # Векторизация
                embeddings = list(model.embed([title]))
                item['vector'] = embeddings[0].tolist() # Берем первый вектор из списка

                producer.send(OUTPUT_TOPIC, value=item)
                producer.flush()
                print(f"Processed: {item.get('id')}")
        except Exception as e:
            print(f"Error processing item: {e}")

if __name__ == "__main__":
    run()
