import csv
import base64
import json
from typing import Dict, Any, List, Callable
from io import StringIO
from google.cloud import pubsub_v1
from asserttions import validate_breadcrumb_1
from asserttions import validate_breadcrumb_2
from asserttions import validate_hdop
from asserttions import validate_coordinates
from asserttions import validate_latitude_range
from asserttions import validate_opd_date_presence
from asserttions import validate_gps_satellites
from asserttions import validate_opd_date_format
from asserttions import validate_meters
from asserttions import validate_act_time
from asserttions import validate_speed_
from transform import process_validated_messages
from insertbreadcrumb import filter_and_copy_to_postgres
from insertTrip import copy_trip_data
import psycopg2
from datetime import datetime
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

validated_messages = []
gcs_messages = []

# Incremented for each message received
message_count = 0

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
    # print(message_count)
    vehicle_id = message.attributes['vehicle_id']
    data = message.data
    if message.attributes.get('base64'):
        data = base64.b64decode(data)
    breadcrumb = json.loads(data)
    validators = [
        validate_breadcrumb_1,
        validate_breadcrumb_2,
        # validate_hdop,
        validate_coordinates,
        validate_latitude_range,
        validate_opd_date_presence,
        validate_gps_satellites,
        validate_opd_date_format,
        validate_meters,
        validate_act_time
    ]
    gcs_messages.append(breadcrumb)
    if validate_message(breadcrumb, validators):
        validated_messages.append(breadcrumb)
        message.ack()
        print(f"Message {message_count} received and validated successfully.")
    else:
        # print(f"Message {message_count} failed validation: {breadcrumb}")
        message.nack()

# Define your subscription details and timeout
project_id = "dataflow-kumari"
subscription_id = "test-1"
timeout = 60.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Subscribe to the topic with the defined callback function
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")


# Call the function to process validated messages
#processed_df = process_validated_messages(validated_messages)

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        # Handle timeout error
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
    finally:
        current_date = datetime.now().strftime("%Y-%m-%d")
        blob_name = f"{current_date}_breadcrumbs.csv"
        store_messages_in_gcs(gcs_messages, blob_name)
        processed_df = process_validated_messages(validated_messages)
        df = validate_speed_(processed_df)
        #print(df)
        columns_to_insert = ['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']
        copy_trip_data(conn, df)
        filter_and_copy_to_postgres(df, 'breadcrumb',columns_to_insert )
        conn.close()

