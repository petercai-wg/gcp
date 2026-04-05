from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import functions_framework

import json
import os
import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT")
QUEUE = os.environ.get("QUEUE_NAME")
LOCATION = os.environ.get("REGION")  ##"us-central1"
WORKER_URL = os.environ.get("WORKER_URL")  # Cloud Run worker URL

client = tasks_v2.CloudTasksClient()


@functions_framework.http
def post_task(request):
    data = request.json
    # request_json = request.get_json()
    delay_seconds = data.get("delay_seconds", 0)

    print(f"Received data: {data}")

    parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE)
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": WORKER_URL,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(data).encode(),
        }
    }

    if delay_seconds > 0:
        future_time = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=delay_seconds
        )
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(future_time)
        schedule_time = timestamp
        task["schedule_time"] = schedule_time

    response = client.create_task(parent=parent, task=task)

    print(f"Task created: {response.name}")

    return response.name, 200
