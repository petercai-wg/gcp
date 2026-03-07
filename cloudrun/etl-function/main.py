import functions_framework

import csv
import io
import os
import sqlalchemy

from google.cloud import storage
from google.cloud.sql.connector import Connector, IPTypes


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def etl_csv_to_cloudsql(cloud_event):

    # Environment variables
    DB_USER = os.environ.get("DB_USER")
    DB_PASS = os.environ.get("DB_PASS")
    DB_NAME = os.environ.get("DB_NAME")
    TABLE_NAME = os.environ.get("TABLE_NAME")
    CLOUD_SQL_CONNECTION_NAME = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket_name = data["bucket"]
    file_name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket_name}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # Initialize Cloud Storage client

    storage_client = storage.Client()

    # Download the CSV content
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob_data = blob.download_as_string()

    print(f"Download file size: {len(blob_data)} bytes")

    # Decode and parse CSV
    s_io = io.StringIO(blob_data.decode("utf-8"))
    reader = csv.reader(s_io)
    next(reader)  # Skip header row

    # The connection name is in the format 'project:region:instance'
    print(
        f"Connecting to Cloud SQL... {CLOUD_SQL_CONNECTION_NAME} with user {DB_USER} and database {DB_NAME}"
    )

    # Connect to Cloud SQL
    unix_socket = f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}/.s.PGSQL.5432"
    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            query={"unix_sock": unix_socket},
        )
    )
    conn = db.connect()

    print(f"starting insert to table: {TABLE_NAME}")

    # Prepare the INSERT statement based on your table schema.
    # Table columns (per table.sql): TransactionDate, SequenceNumber, ServiceProvider,
    # Location, Type, Service, Discount, Amount, Balance
    insert_statement = sqlalchemy.text(
        f"INSERT INTO {TABLE_NAME} ("
        "TransactionDate, SequenceNumber, ServiceProvider, Location, Type, Service, Discount, Amount, Balance) "
        "VALUES (:TransactionDate, :SequenceNumber, :ServiceProvider, :Location, :Type, :Service, :Discount, :Amount, :Balance)"
    )

    rows_inserted = 0
    for row in reader:

        conn.execute(
            insert_statement,
            {
                "TransactionDate": row[0],
                "SequenceNumber": row[1],
                "ServiceProvider": row[2],
                "Location": row[3],
                "Type": row[4],
                "Service": row[5],
                "Discount": row[6],
                "Amount": row[7],
                "Balance": row[8],
            },
        )
        rows_inserted += 1

    conn.commit()
    conn.close()

    print(f"ETL job complete. Inserted {rows_inserted} rows into {TABLE_NAME}.")
