from pyspark.sql import SparkSession


import argparse
import logging

LOGGER: logging.Logger = logging.getLogger("dataproc_serverless")


def main(conf):
    # create Spark Session
    spark = SparkSession.builder.appName(conf.app_name).getOrCreate()
    print("SparkSession created with app name: {}".format(conf.app_name))
    print("Input path: {}".format(conf.input))
    print("Output table: {}".format(conf.output))

    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j_level = "INFO"
    log4j_level_obj = log4j.Level.toLevel(log4j_level)

    log4j.LogManager.getRootLogger().setLevel(log4j_level_obj)
    log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j_level_obj)
    spark.sparkContext.setLogLevel(log4j_level)

    LOGGER.info(f"SparkSession created with app name: {conf.app_name}")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(conf.input)
    )

    df.printSchema()

    df.write.format("bigquery").option("table", conf.output).option(
        "temporaryGcsBucket", conf.temp_gcs_bucket
    ).mode("overwrite").save()

    LOGGER.info(f"Data from {conf.input} written to BigQuery table: {conf.output}")
    spark.stop()


if __name__ == "__main__":
    parser: argparse.ArgumentParser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--app-name",
        dest="app_name",
        type=str,
        default="default",
        help="Application name",
    )
    parser.add_argument("--input", dest="input", type=str, help="Job Input CSV file")
    parser.add_argument(
        "--output", dest="output", type=str, help="Job Output BigQuery table"
    )
    parser.add_argument(
        "--temp-gcs-bucket",
        dest="temp_gcs_bucket",
        type=str,
        help="Temporary GCS bucket for BigQuery operations",
    )

    args = parser.parse_args()
    main(args)


"""
## BigQuery setup
bq mk --location=${REGION}  my_df_etl 
bq mk --table my_df_etl.go_txn \
TransactionDate:TIMESTAMP,SequenceNumber:INTEGER,ServiceProvider:STRING,Location:STRING,Type:STRING,Service:STRING,Discount:FLOAT,Amount:FLOAT,Balance:FLOAT

### Dataproc Severless

curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @pyspark-request.json \
    "https://dataproc.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/batches"


"""
