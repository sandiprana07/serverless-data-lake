
#To check the local gcp authentication programmatically
from google.cloud import storage

client = storage.Client()
buckets = list(client.list_buckets())
print([b.name for b in buckets])
