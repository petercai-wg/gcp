
gcloud builds submit --tag $DOCKER_REGISTRY/$PROJECT_ID/my-registry/sms-service

gcloud run deploy sms-service \
  --image $DOCKER_REGISTRY/$PROJECT_ID/my-registry/sms-service --region $REGION \
  --platform managed --no-allow-unauthenticated --max-instances=1

