import pendulum

from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

GCP_PROJECT_ID = Variable.get("gcp_project_id")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
GCS_BUCKET = Variable.get("gcp_bucket")
# Dataflow Template details for raw transaction validation
DATAFLOW_TEMPLATE_PATH = f"gs://dataflow-templates-{GCP_REGION}/latest/GCS_Text_to_BigQuery"
DATAFLOW_PY_TRANSFORM = f"gs://{GCS_BUCKET}/dataflow/transforms/validate_transactions.py"
# Serverless Spark PySpark file for aggregation
SPARK_AGG_SCRIPT = f"gs://{GCS_BUCKET}/spark/aggregate_sales.py"
# BigQuery dataset and tables
BQ_DATASET = "cymbal_financials"
BQ_RAW_TABLE = "raw_transactions"
BQ_AGG_TABLE = "daily_aggregated_sales"
BQ_FINAL_REPORT_TABLE = "daily_sales_report"

--- DAG DEFINITION ---
with DAG(
    dag_id="cymbal_financial_daily_processing",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["cymbal_superstore", "dataflow", "spark"],
    default_args={
        "owner": "Cymbal Data Engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    # Task 1: Ingest and validate raw sales data from GCS to BigQuery using a Dataflow Template.
    ingest_raw_transactions = DataflowTemplatedJobStartOperator(
        task_id="ingest_raw_transactions",
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        template=DATAFLOW_TEMPLATE_PATH,
        parameters={
            "inputFilePattern": f"gs://{GCS_BUCKET}/sales/raw/{{{{ ds }}}}/transactions.csv",
            "JSONPath": f"gs://{GCS_BUCKET}/dataflow/schemas/transaction_schema.json",
            "outputTable": f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}",
            "bigQueryLoadingTemporaryDirectory": f"gs://{GCS_BUCKET}/temp/bq-load/",
            "pythonUserDefinedFunctions": DATAFLOW_PY_TRANSFORM,
            "userDefinedFunctionName": "validateAndTransform",
            "BQ_RAW_TABLE_ID": f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}",
        },
    )

    # Task 2: Run a Serverless Spark job to perform complex aggregations on the raw data.
    aggregate_with_spark = DataprocSubmitJobOperator(
        task_id="aggregate_with_spark",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": "ephemeral-spark-cluster-{{ ds_nodash }}"},
            "reference": {"project_id": GCP_PROJECT_ID},
            "pyspark_job": {
                "main_python_file_uri": SPARK_AGG_SCRIPT,
                "args": [
                    f"--date={{{{ ds }}}}",
                    f"--source_table={BQ_RAW_TABLE_ID}",
                    f"--destination_table={GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_AGG_TABLE}"
                ],
            },
        },
         gcp_conn_id="google_cloud_default" # Explicitly define connection
    )

    # Task 3: Load the final, aggregated data into the main reporting table.
    load_to_reporting_table = BigQueryInsertJobOperator(
        task_id="load_to_reporting_table",
        gcp_conn_id="google_cloud_default",
        sql=f"""
            MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_FINAL_REPORT_TABLE}` T
            USING `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_AGG_TABLE}` S
            ON T.transaction_date = S.transaction_date AND T.product_id = S.product_id -- Or other unique key
            WHEN NOT MATCHED BY TARGET THEN
              INSERT (transaction_date, product_id, total_sales, total_quantity)
              VALUES(S.transaction_date, S.product_id, S.total_sales, S.total_quantity)
            WHEN MATCHED THEN
              UPDATE SET
                T.total_sales = S.total_sales,
                T.total_quantity = S.total_quantity
        """,
    )


    # --- TASK DEPENDENCIES ---
    ingest_raw_transactions >> aggregate_with_spark >> load_to_reporting_table

