gcloud builds submit --tag $DOCKER_REGISTRY/$PROJECT_ID/my-registry/lab-report-service

gcloud run deploy lab-report-service \
  --image $DOCKER_REGISTRY/$PROJECT_ID/my-registry/lab-report-service --region $REGION \
  --platform managed --no-allow-unauthenticated --max-instances=1


