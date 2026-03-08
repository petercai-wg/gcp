import argparse
import time
import logging
import csv
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

### python3 -m pip install apache-beam[gcp]


def _parse_csv_line(line, fieldnames, delimiter):
    # Use Python csv module to properly handle quoted fields.
    row = next(csv.reader([line], delimiter=delimiter))
    # If the CSV line has fewer columns than expected, pad with empty strings.
    if len(row) < len(fieldnames):
        row += [""] * (len(fieldnames) - len(row))
    return dict(zip(fieldnames, row))


def _cast_types(row):
    # Adjust casting here to match your BigQuery schema.
    # Empty strings are converted to None to allow NULL values in BigQuery.
    def _to_float(val):
        return float(val) if val not in (None, "") else None

    def _to_int(val):
        return int(val) if val not in (None, "") else None

    def _to_timestamp(val):
        if val is None or val == "":
            return None
        # BigQuery accepts both ISO8601 (with 'T') and space-separated formats.
        # Replace the 'T' with a space to match "YYYY-MM-DD HH:MM:SS" formatting.
        return val.replace("T", " ")

    return {
        "TransactionDate": _to_timestamp(row.get("TransactionDate")),
        "SequenceNumber": _to_int(row.get("SequenceNumber")),
        "ServiceProvider": row.get("ServiceProvider"),
        "Location": row.get("Location"),
        "Type": row.get("Type"),
        "Service": row.get("Service"),
        "Discount": _to_float(row.get("Discount")),
        "Amount": _to_float(row.get("Amount")),
        "Balance": _to_float(row.get("Balance")),
    }


def run():
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="Load CSV data into BigQuery")
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

    parser.add_argument(
        "--input",
        required=False,
        default=None,
        help="GCS path to input CSV file (e.g. gs://<bucket>/data.csv)",
    )
    parser.add_argument(
        "--output",
        required=False,
        default=None,
        help="BigQuery table to write to (e.g. <project>:<dataset>.<table>)",
    )
    parser.add_argument(
        "--delimiter",
        required=False,
        default=",",
        help='Delimiter to use when parsing CSV (default=",")',
    )
    parser.add_argument(
        "--skipHeader",
        action="store_true",
        default=True,
        help="Skip the first line of the CSV file (useful when the file has a header)",
    )

    opts = parser.parse_args()

    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = "{0}{1}".format(
        "csv-to-bq-", time.time_ns()
    )
    options.view_as(StandardOptions).runner = opts.runner

    # Default input/output if not provided
    input_path = opts.input or f"gs://{opts.project}/data.csv"
    output_table = opts.output or f"{opts.project}:my_df_etl.go_txn"

    # Define BigQuery schema (matches the schema in bq.json).
    table_schema = {
        "fields": [
            {"name": "TransactionDate", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "SequenceNumber", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "ServiceProvider", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Service", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Discount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Balance", "type": "FLOAT", "mode": "NULLABLE"},
        ]
    }

    fieldnames = [
        "TransactionDate",
        "SequenceNumber",
        "ServiceProvider",
        "Location",
        "Type",
        "Service",
        "Discount",
        "Amount",
        "Balance",
    ]

    p = beam.Pipeline(options=options)

    read_args = {}
    if opts.skipHeader:
        logging.info("RReading CSV and skipping header line ...")
        # skip_header_lines is supported by ReadFromText
        read_args["skip_header_lines"] = 1

    (
        p
        | "ReadFromGCS" >> beam.io.ReadFromText(input_path, **read_args)
        | "ParseCsv"
        >> beam.Map(_parse_csv_line, fieldnames=fieldnames, delimiter=opts.delimiter)
        | "CastTypes" >> beam.Map(_cast_types)
        | "WriteToBQ"
        >> beam.io.WriteToBigQuery(
            output_table,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )
    )

    logging.info("Building pipeline ...")

    p.run()

    logging.info("Finished pipeline execution.")


if __name__ == "__main__":
    run()

"""
python dataflow_csv.py \
  --project=${PROJECT_ID} --region=us-central1 \
  --stagingLocation=gs://peter_dataflow_bucket/temp/ \
  --tempLocation=gs://peter_dataflow_bucket/tmp/ \
  --input=gs://peter_dataflow_bucket/csv/go_txn/go-202601.csv \
  --output=${PROJECT_ID}:my_df_etl.go_txn \
  --delimiter=, --skipHeader \
  --runner=DataflowRunner

  --runner=DataflowRunner   ## run the pipeline on Dataflow.
  --runner=DirectRunner  ### simple read the data
  
"""

"""'
#/bin/sh
pip3 install -q anytree
pip3 install -q --upgrade google-cloud-pubsub
pip3 install -q faker
pip3 install -q --upgrade geocoder

"""
