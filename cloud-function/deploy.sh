cd ./cloud-function

#deploying cloud function in GCP from CLI

gcloud functions deploy publish_traffic_data \
  --gen2 \
  --runtime python310 \
  --region asia-south1 \
  --entry-point publish_traffic_data \
  --trigger-event google.storage.object.finalize \
  --trigger-resource traffic-data-raw-bucket \
  --allow-unauthenticated
  
#Need to provide below service role for event based triggers
gcloud projects add-iam-policy-binding certain-mission-456820-a8 \
  --member="serviceAccount:service-735931915131@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"