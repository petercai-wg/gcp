import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

# ### main


def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description="Load from Json into BigQuery")
    parser.add_argument("--project", required=True, help="Specify Google Cloud project")
    parser.add_argument("--region", required=True, help="Specify Google Cloud region")
    parser.add_argument(
        "--stagingLocation",
        required=True,
        help="Specify Cloud Storage bucket for staging",
    )
    parser.add_argument(
        "--tempLocation", required=True, help="Specify Cloud Storage bucket for temp"
    )
    parser.add_argument("--runner", required=True, help="Specify Apache Beam Runner")

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = "{0}{1}".format(
        "my-pipeline-", time.time_ns()
    )
    options.view_as(StandardOptions).runner = opts.runner

    # Static input and output
    input = "gs://{0}/events.json".format(opts.project)
    output = "{0}:logs.logs".format(opts.project)

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {"name": "ip", "type": "STRING", "mode": "REQUIRED"},
            {"name": "user_id", "type": "STRING"},
            {"name": "lat", "type": "FLOAT"},
            {"name": "lng", "type": "FLOAT"},
            {"name": "timestamp", "type": "STRING"},
            {"name": "http_request", "type": "STRING"},
            {"name": "http_response", "type": "INTEGER"},
            {
                "name": "num_bytes",
                "type": "INTEGER",
                "mode": "NULLABLE",
            },
            {"name": "user_agent", "type": "STRING"},
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    """
    Steps:
    1) Read something
    2) Transform something
    3) Write something
    """

    (
        p
        | "ReadFromGCS" >> beam.io.ReadFromText(input)
        | "ParseJson" >> beam.Map(lambda line: json.loads(line))
        | "WriteToBQ"
        >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()


if __name__ == "__main__":
    run()

    """

# Run the pipelines
python3 dataflow_etl.py \
  --project=${PROJECT_ID} \
  --region=us-central1 \
  --stagingLocation=gs://$PROJECT_ID/staging/ \
  --tempLocation=gs://$PROJECT_ID/temp/ \
  --runner=DataflowRunner

## show table schema
bq show --schema --format=prettyjson logs.logs  

{"BigQuery Schema":[
  {
    "mode": "NULLABLE",
    "name": "ip",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "lat",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "lng",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "timestamp",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "http_request",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "http_response",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "num_bytes",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "user_agent",
    "type": "STRING"
  }
]}


{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:49:42.746115Z", "http_request": "\"GET home.html HTTP/1.0\"", "http_response": 200, "num_bytes": 291, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:49:44.028593Z", "http_request": "\"GET home.html HTTP/1.0\"", "http_response": 200, "num_bytes": 474, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:49:51.442576Z", "http_request": "\"GET archea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 425, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:49:55.030289Z", "http_request": "\"GET archaea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 269, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:49:57.750073Z", "http_request": "\"GET archaea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 163, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:50:05.585048Z", "http_request": "\"GET archaea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 383, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:50:06.610474Z", "http_request": "\"GET archaea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 341, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:50:11.262891Z", "http_request": "\"GET archea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 255, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:50:16.272031Z", "http_request": "\"GET home.html HTTP/1.0\"", "http_response": 200, "num_bytes": 176, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}
{"ip": "118.158.170.224", "user_id": "-1567246866124788306", "lat": 35.6895, "lng": 139.6917, "timestamp": "2026-03-05T21:50:25.468382Z", "http_request": "\"GET archea.html HTTP/1.0\"", "http_response": 200, "num_bytes": 485, "user_agent": "Mozilla/5.0 (Windows 98; Win 9x 4.90; be-BY; rv:1.9.1.20) Gecko/3725-10-13 19:18:59.723000 Firefox/3.6.8"}

    """
