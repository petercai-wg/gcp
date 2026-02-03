 
 gcloud functions deploy emailfunction \
  --gen2 \
  --runtime=nodejs24 --region=$REGION \
  --source=. --entry-point=emailPubSub  \
  --trigger-topic=my-lab-topic \
  --max-instances 1  \
  --service-account=987799526902-compute@developer.gserviceaccount.com 