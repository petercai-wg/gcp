curl -X POST -H "Content-Type: application/json" \
  -d "{\"id\": 21, \"status\": \"new\"}" -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  $LAB_REPORT_SERVICE_URL 

###  this doesn't work
curl -X POST -H "Content-Type: application/json" \
  -d "{\"id\": 14, \"status\": \"update\"}" -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  $LAB_REPORT_SERVICE_URL   