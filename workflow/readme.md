# GCP workflow demo
## Description
Use Workflows with Cloud Run and Cloud Run functions

* Deploy two Cloud Run functions: the first function generates a random number, and then passes that number to the second function which multiplies it.
* Connect an external HTTP API that returns the log for a given number
* Deploy a Cloud Run service that allows authenticated access only. The service returns the math.floor for a given number.
* Using Workflows, connect all services, execute the entire workflow, and return a final result.


### Executing program

gcloud config set project PROJECT_ID

export PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")
export PROJECT_ID=$(gcloud config get-value project)
export REGION=northamerica-northeast2

gcloud config set functions/region ${REGION}
gcloud config set run/region ${REGION}
gcloud config set workflows/location ${REGION}

export SERVICE_ACCOUNT=workflows-sa
gcloud iam service-accounts create ${SERVICE_ACCOUNT}

gcloud projects add-iam-policy-binding PROJECT_ID \
    --member "serviceAccount:${SERVICE_ACCOUNT}@PROJECT_ID.iam.gserviceaccount.com"  --role "roles/run.invoker"

gcloud functions deploy randomgen-function    --gen2     --runtime python310 --region=northamerica-northeast2 --project=$PROJECT_ID \
    --entry-point=randomgen     --trigger-http     --allow-unauthenticated

gcloud functions deploy multiply-function    --gen2     --runtime python310 --region=northamerica-northeast2 --project=$PROJECT_ID \
    --entry-point=multiply     --trigger-http     --allow-unauthenticated    

export SERVICE_NAME=floor
gcloud builds submit --tag northamerica-northeast2-docker.pkg.dev/$PROJECT_ID/my-registry/floor .

gcloud run deploy my-floor  --image northamerica-northeast2-docker.pkg.dev/$PROJECT_ID/my-registry/floor:latest --no-allow-unauthenticated

gcloud workflows deploy my_fn_workflow  --source=workflow.yaml     --service-account=workflows-sa@$PROJECT_ID.iam.gserviceaccount.com --project=$PROJECT_ID

### test 
gcloud workflows run my_fn_workflow --project=$PROJECT_ID
