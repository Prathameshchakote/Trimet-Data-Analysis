import json
import logging
import pandas as pd
import requests
import base64
from google.cloud import pubsub_v1
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Cloud project and topic information
project_id = "dataflow-kumari"
topic_id = "test"
topic_path = f"projects/{project_id}/topics/{topic_id}"

# Initialize Google Cloud Pub/Sub client
publisher = pubsub_v1.PublisherClient()

# Set to store processed vehicle IDs
processed_vehicle_ids = set()

# Function to chunk a list into batches
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def get_response_details(vehicle_id):
    """Fetches breadcrumbs data for a specific vehicle ID from the bus data API.

    Args:
        vehicle_id (int): The ID of the vehicle.

    Returns:
        tuple: A tuple containing the HTTP status code and the JSON response data.
    """

    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for non-2xx responses
        return response.status_code, response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching data for vehicle ID {vehicle_id}: {e}")
        return None, None


def publish_to_pubsub(vehicle_id, event):
    """Fetches breadcrumbs data for a specific vehicle ID from the bus data API.

    Args:
        vehicle_id (int): The ID of the vehicle.

    Returns:
        tuple: A tuple containing the HTTP status code and the JSON response data.
    """
    try:
        message_data = json.dumps(event).encode("utf-8")
        future = publisher.publish(
            topic_path, data=message_data, vehicle_id=str(vehicle_id)
        )
        future.result()  # Block until the message is published
        logger.info(f"Published message for vehicle ID: {vehicle_id}")
    except Exception as e:
        logger.error(f"Error publishing message for vehicle ID {vehicle_id}: {e}")


def process_vehicle(vehicle_id):
    if vehicle_id in processed_vehicle_ids:
        logger.info(
            f"Data for vehicle ID {vehicle_id} has already been sent. Skipping."
        )
        return

    status_code, content = get_response_details(vehicle_id)
    if status_code == 200 and content:
        logger.info(f"Vehicle ID: {vehicle_id}, Response Status: {status_code}")
        for event in content:
            publish_to_pubsub(vehicle_id, event)

        processed_vehicle_ids.add(vehicle_id)  # Mark vehicle ID as processed
    else:
        logger.warning(
            f"Failed to fetch data for vehicle ID {vehicle_id}. Status Code: {status_code}"
        )


def main():
    # Define the list of vehicle IDs
    vehicle_ids = [
        '3940', '3137', '3513', '3905', '3220', '3415', '3157', '3732', '3543', '4035', '3924', '3540', '3227', 
        '4237', '4039', '3247', '3166', '3209', '3722', '3950', '3925', '3512', '3956', '3560', '2909', '2933', 
        '3235', '3261', '3556', '4050', '3241', '3749', '3154', '3959', '3149', '3143', '3237', '3017', '2910', 
        '3511', '3571', '3954', '4516', '3055', '3625', '3907', '4518', '3946', '3729', '3634', '3952', '3918', 
        '3527', '3728', '3410', '3719', '3254', '3516', '3508', '3928', '3028', '3707', '4525', '3549', '3741', 
        '4001', '3529', '3915', '3322', '3040', '2926', '3510', '3943', '3957', '3562', '3702', '3039', '3648', 
        '3909', '3505', '3226', '3134', '3216', '3120', '3020', '3620', '4028', '2908', '3731', '3320', '3401', 
        '3617', '3101', '3921', '4522', '2901', '3727', '3605', '3746', '4210'
    ]

    column_name = "Giggles"
    

    if not vehicle_ids:
        logger.warning("No vehicle IDs found in the list.")
        return

    threads = []
    for vehicle_id in vehicle_ids:
        thread = threading.Thread(target=process_vehicle, args=(vehicle_id,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
