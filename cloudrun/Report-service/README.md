# GCP cloud run with pub/sub

## Overview
- Simple Cloud Run, connected with Pub/Sub topic
- Cloud Run IAM authentication (global)

    service-{prj-num}@gcp-sa-pubsub.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator
- Create subscription for cloud run push style

    gcloud pubsub subscriptions create {sub-name} --topic {topic-name} 
    --push-endpoint={cloudRun EndPoint}
    --push-auth-service-account={svr-account}

- deploy functions to subscribe to topic

    gcloud functions deploy emailPubSub --trigger-topic $FUNCTIONS_TOPIC 
    --runtime [YOUR_RUNTIME] 
      --source=.  --entry-point=addToRedis \
    --trigger-topic=$TOPIC \
    --max-instances 1   