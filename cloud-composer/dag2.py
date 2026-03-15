from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.dates import days_ago

with DAG(
    dag_id="hourly_spark_batch_pipeline",
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["dataproc", "spark", "batch"],
) as dag:

    start_spark_batch_job = DataprocCreateBatchOperator(
        task_id="run_spark_transformation",
        project_id="your-gcp-project-id",
        region="your-gcp-region",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://your-code-bucket/spark_job.py",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "your-service-account@your-project.iam.gserviceaccount.com",
                },
            },
        },
    )
