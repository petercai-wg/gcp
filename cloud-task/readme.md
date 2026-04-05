# GCP cloud task demo
## Description

The core problem Cloud Tasks solves:
* 1. decouple task
* 2. retry
* 3. Rate limite, traffic control
* 4. sechduled execution

### Executing program

gcloud config set project PROJECT_ID
export PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")
export PROJECT_ID=$(gcloud config get-value project)


gcloud services enable cloudtasks.googleapis.com

export SERVICE_ACCOUNT=cloudtask-sa
gcloud iam service-accounts create ${SERVICE_ACCOUNT}

gcloud projects add-iam-policy-binding $PROJECT_ID    --member "serviceAccount:cloudtask-sa@$PROJECT_ID.iam.serviceaccount.com"  --role=roles/cloudtasks.admin

gcloud projects add-iam-policy-binding $PROJECT_ID    --member "serviceAccount:cloudtask-sa@$PROJECT_ID.iam.serviceaccount.com"  --role "roles/run.invoker"


1. Create Cloud Tasks Queue
gcloud tasks queues create my-task-queue --location=us-central1 \
    --max-attempts=5 \
    --min-backoff="2s" \
    --max-backoff="60s" \
    --max-doublings=3 \
    --max-retry-duration="3600s" \
  --max-dispatches-per-second=5 \
  --max-concurrent-dispatches=2 \
  --log-sampling-ratio=1.0


2. deploy httpwork as cloud run services

gcloud run deploy http-worker-service  --source .  --region=us-central1   --allow-unauthenticated \
  --service-account=cloudtask-sa@$PROJECT_ID.iam.gserviceaccount.com \
   --project=$PROJECT_ID 

3. deploy front end API as cloud Function
gcloud functions deploy task-function    --gen2     --runtime python310 --region=us-central1 --project=$PROJECT_ID \
    --entry-point=post_task     --trigger-http     --allow-unauthenticated    \
    --service-account=cloudtask-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars GCP_PROJECT=$PROJECT_ID,REGION=us-central1,QUEUE_NAME=my-task-queue,WORKER_URL=https://http-worker-service-$PROJECT_NUMBER.us-central1.run.app



### test 
curl -X POST https://task-function-$PROJECT_NUMBER.us-central1.run.app   -H "Content-Type: application/json"     -d '{"delay_seconds": 5, "input": 1}'    
