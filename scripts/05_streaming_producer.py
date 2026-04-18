import json
import time
import pandas as pd
from confluent_kafka import Producer
from pathlib import Path

config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(config)
TOPIC_NAME = 'uk_property_sales'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
base_path = Path(__file__).resolve().parent.parent
csv_path = base_path / 'data' / 'raw' / 'pp-complete.csv'

print(f"Starting data streaming from: {csv_path}")

try:
    for chunk in pd.read_csv(csv_path, chunksize=5, names=[
        'id', 'price', 'sale_date', 'postcode', 'type', 'new_build', 
        'duration', 'paon', 'saon', 'street', 'locality', 'city', 
        'district', 'county', 'p_type', 'record_status'
    ]):
        for index, row in chunk.iterrows():
            payload = row.to_dict()
            producer.produce(
                TOPIC_NAME, 
                json.dumps(payload).encode('utf-8'), 
                callback=delivery_report
            )
        
        producer.flush()
        print("Batch sent. Sleeping for 2 seconds...")
        time.sleep(2)

except FileNotFoundError:
    print(f"Error: The file {csv_path} was not found. Current path: {Path.cwd()}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("Streaming finished.")