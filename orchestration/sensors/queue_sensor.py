from dagster import sensor, RunRequest, SensorEvaluationContext, AssetSelection
from confluent_kafka import Consumer
import logging

@sensor(target=AssetSelection.assets("raw_stream_ingestion"))
def redpanda_message_sensor(context: SensorEvaluationContext):
    """
    Polls Redpanda to check if there are any new messages.
    If messages exist, it triggers the 'raw_stream_ingestion' asset.
    """
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dagster_sensor_group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['uk_property_sales'])
    
    msg = consumer.poll(timeout=1.0)
    consumer.close()

    if msg is not None and not msg.error():
        yield RunRequest(
            run_key=None,
            tags={"source": "redpanda_sensor"}
        )
    else:
        context.log.info("No new messages in Redpanda. Skipping run.")