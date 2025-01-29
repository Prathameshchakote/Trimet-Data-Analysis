from google.cloud import storage
from io import StringIO
import csv
import zlib
from datetime import datetime
from typing import List, Dict, Any

def store_messages_in_gcs(messages: List[Dict[str, Any]],blob_name) -> None:
    """Stores collected breadcrumb messages into a GCS bucket."""
    if not messages:
        return

    bucket_name = "activity_2"
    # Initialize the GCS client and get the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    print("blob")
    print(blob)
    
    # Convert the collected messages to CSV format
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=messages[0].keys())
    csv_writer.writeheader()
    csv_writer.writerows(messages)
    
    # Compress the CSV data
    csv_data = csv_buffer.getvalue().encode('utf-8')
    compressed_data = zlib.compress(csv_data)
    
    # Upload the compressed CSV file to GCS
    blob.upload_from_string(compressed_data, content_type='application/octet-stream')
    print(f"Batch file uploaded to {bucket_name}/{blob_name}")

    # Calculate and print the size of the original and compressed data
    original_size = len(csv_data)
    compressed_size = len(compressed_data)
    print(f"Original size: {original_size / 1024:.2f} KiB")
    print(f"Compressed size: {compressed_size / 1024:.2f} KiB")

