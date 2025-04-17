# To Setup the project id , region, zone and athentication
gcloud init

#create a bucket 'traffic-data-raw-bucket'
gsutil mb -l asia-south1 gs://traffic-data-raw-bucket/

# command to create the pub/sub topic
gcloud pubsub topics create traffic-data-topic

#topic path after successfully creation
#projects/certain-mission-456820-a8/topics/traffic-data-topic

#to install apache beam package for gcp data-flow
pip install apache-beam[gcp]

#command to create the 'dataset' in Bigquery
bq mk \
  --dataset \
  --location=asia-south1 \
  certain-mission-456820-a8:traffic_dataset

