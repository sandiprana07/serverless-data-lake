import csv
import time
import tempfile
from google.cloud import storage, pubsub_v1

# Create Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('certain-mission-456820-a8', 'traffic-data-topic')

print(f"topic path: {topic_path}")

def publish_traffic_data(event, context):
    """Triggered by a file upload to Cloud Storage."""
    bucket_name = event['bucket']
    file_name = event['name']

    print(f"bucket name is: {bucket_name}")
    print(f"file name: {file_name}")

    if not file_name.endswith('.csv'):
        print(f"Ignoring non-CSV file: {file_name}")
        return

    print(f"Processing file: gs://{bucket_name}/{file_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
        blob.download_to_file(temp_file)
        temp_file.seek(0)

        reader = csv.DictReader(line.decode('utf-8') for line in temp_file)
        for row in reader:
            print(f"row: {str(row)}")
            publisher.publish(topic_path, str(row).encode('utf-8'))
            time.sleep(1)  # Optional: simulate 1 row/sec
