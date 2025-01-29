import csv
import base64
import json
import os
from typing import Dict, Any, List, Callable
from io import StringIO
from google.cloud import pubsub_v1
from datetime import datetime
from assertions_for_stop_event import validate_records
from assertions_for_stop_event import check_numeric_field
from assertions_for_stop_event import validate_direction
from assertions_for_stop_event import validate_service_key
from create_event_info import copy_from_dataframe
from assertions_for_stop_event import validate_vehicle_id
import psycopg2
import pandas as pd 
from archiver import store_messages_in_gcs
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='8252',
    host='localhost',
    port='5432'
)
conn.autocommit = True
cur = conn.cursor()

message_count = 0
validated_messages = []
messages = []
# Create the data directory if it doesn't exist
os.makedirs('data', exist_ok=True)
def validate_message(message: Dict[str, Any], validators: List[Callable[..., bool]]) -> bool:
    """Validate a message using a list of validation functions.

    Args:
        message: The message to be validated.
        validators: A list of validation functions.

    Returns:
        bool: True if all validations pass, False otherwise.
    """
    for validator in validators:
        if not validator(message):
            return False
    return True
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global message_count
    message_count += 1

    vehicle_id = message.attributes['vehicle_id']
    data = message.data
    if message.attributes.get('base64'):
        data = base64.b64decode(data)
    breadcrumb = json.loads(data)
    validators = [validate_records,
    check_numeric_field,
    validate_direction,
    validate_service_key,
    validate_vehicle_id]
    messages.append(breadcrumb)
    if validate_message(breadcrumb, validators):
        validated_messages.append(breadcrumb)
        message.ack()
        print(f"Message {message_count} received and validated successfully.")
    else:
        print("Message {message_count} failed validation")
        message.nack()
    

# Define your subscription details and timeout
project_id = "dataflow-kumari"
subscription_id = "sample-sub"
timeout = 300.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)



# Subscribe to the topic with the defined callback function
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
    finally:
        current_date = datetime.now().strftime("%Y-%m-%d")
        blob_name = f"{current_date}_stop_event_.csv"
        store_messages_in_gcs(messages, blob_name)
        validated_messages = pd.DataFrame(validated_messages)

        column_mapping = {
    'vehicle_number': 'vehicle_id',
    'route_number': 'route_id',
    'direction': 'direction',
    'service_key': 'service_key',
    'PDX_TRIP': 'trip_id'
}       

        table_name = 'event_info'
        validated_messages = validated_messages.rename(columns=column_mapping)
        copy_from_dataframe(conn, validated_messages, table_name)
        conn.close()
        print(validated_messages)
