import json
import duckdb
import pandas as pd
from dagster import asset, AssetExecutionContext
from confluent_kafka import Consumer
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "nieruchomosci_uk.db"

@asset(group_name="streaming")
def raw_stream_ingestion(context: AssetExecutionContext):
    """
    Consumes messages from Redpanda and appends them to DuckDB.
    Specifies columns to handle the auto-timestamp (ingested_at).
    """
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dagster_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['uk_property_sales'])
    
    con = duckdb.connect(str(DB_PATH))
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_stream_sales (
            id VARCHAR,
            price INTEGER,
            sale_date VARCHAR,
            postcode VARCHAR,
            type VARCHAR,
            new_build VARCHAR,
            duration VARCHAR,
            paon VARCHAR,
            saon VARCHAR,
            street VARCHAR,
            locality VARCHAR,
            city VARCHAR,
            district VARCHAR,
            county VARCHAR,
            p_type VARCHAR,
            record_status VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    messages = []
    max_messages = 50 
    
    context.log.info("Starting to consume messages from Redpanda...")
    
    try:
        for _ in range(max_messages):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                context.log.error(f"Consumer error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            messages.append(data)

        if messages:
            df = pd.DataFrame(messages)
            
            column_names = (
                "id, price, sale_date, postcode, type, new_build, duration, "
                "paon, saon, street, locality, city, district, county, p_type, record_status"
            )
            
            con.execute(f"INSERT INTO bronze_stream_sales ({column_names}) SELECT * FROM df")
            
            context.log.info(f"Successfully ingested {len(messages)} records into bronze_stream_sales.")
        else:
            context.log.info("No new messages found in the queue.")

    except Exception as e:
        context.log.error(f"Error during ingestion: {e}")
        raise e
    finally:
        consumer.close()
        con.close()