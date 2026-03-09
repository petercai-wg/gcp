gcloud functions deploy loadBigQueryFromGCS   --gen2   --runtime nodejs24 --allow-unauthenticated  \
    --source . \
    --region $REGION \
    --trigger-resource gs://my_project1_bucket \
    --trigger-event google.storage.object.finalize \
    --memory=512Mi \
    --timeout=540s \
    --service-account=$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
    --set-env-vars DATASET="my_df_etl",TABLE="go_txn"