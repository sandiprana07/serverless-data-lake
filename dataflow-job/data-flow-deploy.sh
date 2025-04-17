######### one time configuration for data-flow job ################ 
#Application Default Credentials authentication is required to run the data flow job from cli.
gcloud auth login
gcloud auth application-default login
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json"

#Adding the below service role to access the google storage for temp and staging the data
gcloud iam service-accounts add-iam-policy-binding \
  735931915131-compute@developer.gserviceaccount.com \
  --member="serviceAccount:service-735931915131@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

################### every time when you want to run the job ##################
# change pwd to python script folder
 cd ./dataflow-job

# To run the data-flow job from cli
python3 dataflow_job.py \
  --runner DataflowRunner \
  --project=certain-mission-456820-a8 \
  --region=asia-south1 \
  --temp_location=gs://traffic-data-raw-bucket/temp \
  --staging_location=gs://traffic-data-raw-bucket/staging \
  --job_name=traffic-data-pipeline-$(date +%s) \
  --max_num_workers=10 \
  --autoscaling_algorithm=THROUGHPUT_BASED

#To list the data-flow job with status and id
gcloud dataflow jobs list --region=asia-south1

#To cancel the running job with job-id
gcloud dataflow jobs cancel 2025-04-16_02_13_51-3328949246673108747 --region=asia-south1