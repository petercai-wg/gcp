gcloud builds submit --tag $DOCKER_REGISTRY/$PROJECT_ID/my-registry/email-service

gcloud run deploy email-service \
  --image $DOCKER_REGISTRY/$PROJECT_ID/my-registry/email-service --region $REGION \
  --platform managed --no-allow-unauthenticated --max-instances=1


