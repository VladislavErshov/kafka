import requests
import os
from clickhouse_connect import get_client

def main():
    host = os.getenv('CH_HOST', 'ClickHouse')
    port = int(os.getenv('CH_PORT', '8123'))
    user = os.getenv('CH_USER', 'admin')
    password = os.getenv('CH_PASSWORD', '12345678')
    limit = int(os.getenv('HN_LIMIT', '100'))

    top_url = f"{os.getenv('HN_TOP_URL')}/v0/topstories.json"
    item_base_url = f"{os.getenv('HN_ITEM_URL')}/v0/item/"

    # Initialize ClickHouse client
    client = get_client(host=host, port=port, username=user, password=password)

    # Create table schema if not exists
    client.command('''
        CREATE TABLE IF NOT EXISTS hackernews (
            id UInt64,
            title String,
            url String,
            score Int32,
            by String,
            time DateTime
        ) ENGINE = MergeTree() ORDER BY id
    ''')

    print(f"Fetching top {limit} IDs from {top_url}...")
    try:
        top_ids = requests.get(top_url).json()[:limit]
    except Exception as e:
        print(f"Failed to fetch top stories: {e}")
        return

    data_batch = []
    for i, item_id in enumerate(top_ids):
        try:
            item = requests.get(f"{item_base_url}{item_id}.json").json()
            if item and 'title' in item:
                data_batch.append([
                    item.get('id'),
                    item.get('title', ''),
                    item.get('url', ''),
                    item.get('score', 0),
                    item.get('by', 'unknown'),
                    item.get('time', 0)
                ])
            if i % 100 == 0 and i > 0:
                print(f"Loaded {i} items...")
        except Exception as e:
            print(f"Error fetching item {item_id}: {e}")

    # Bulk insert into ClickHouse
    if data_batch:
        client.insert('hackernews', data_batch,
                     column_names=['id', 'title', 'url', 'score', 'by', 'time'])
        print(f"Successfully ingested {len(data_batch)} items into ClickHouse!")

if __name__ == "__main__":
    main()
