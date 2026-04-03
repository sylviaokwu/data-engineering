#!/usr/bin/env python3
"""
PySpark job: Load crypto Parquet from GCS → BigQuery
"""

from pyspark.sql import SparkSession
from datetime import datetime
import os

GCS_BUCKET   = os.getenv("GCS_BUCKET_NAME", "terraform-sylvia-data")
GCP_PROJECT  = os.getenv("GCP_PROJECT_ID", "terraform-sylvia")
BQ_DATASET   = os.getenv("GCP_DATASET", "crypto_dataset")
BQ_TABLE     = "crypto_prices"
DATE_STR     = datetime.today().strftime("%Y-%m-%d")
GCS_PATH     = f"gs://{GCS_BUCKET}/raw/crypto/{DATE_STR}/crypto_prices.parquet"


def main():
    spark = SparkSession.builder \
        .appName("load_crypto_to_bq") \
        .config("spark.jars", "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar,"
                               "/opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"📥 Reading from: {GCS_PATH}")
    df = spark.read.parquet(GCS_PATH)
    df.printSchema()
    print(f"Rows loaded: {df.count()}")

    print(f"📤 Writing to BigQuery: {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    df.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}") \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .mode("append") \
        .save()

    print("✅ Done!")
    spark.stop()


if __name__ == "__main__":
    main()
