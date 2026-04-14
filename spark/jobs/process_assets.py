#!/usr/bin/env python3
"""
Spark processing job: raw/prices/ → processed/crypto/
Reads all parquet files from GCS raw layer, filters to crypto,
cleans, enriches with features, and writes partitioned output.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



GCS_BUCKET   = os.getenv("GCS_BUCKET_NAME", "terraform-sylvia-data")
GCP_PROJECT  = os.getenv("GCP_PROJECT_ID")
BQ_DATASET   = os.getenv("GCP_DATASET", "de_staging")
BQ_TABLE     = f"{GCP_PROJECT}.{BQ_DATASET}.asset_prices"
RAW_PATH     = f"gs://{GCS_BUCKET}/raw/prices/"


# ── Spark Session ──────────────────────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoProcessing")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopAbstractFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/spark/secrets/gcp-key.json")
        .config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS",
                "/opt/spark/secrets/gcp-key.json")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/spark/secrets/gcp-key.json") 
        .getOrCreate()
    )


# ── Transformations ────────────────────────────────────────────────────────────

def read_raw(spark):
    logger.info(f"Reading raw parquet from {RAW_PATH}")
    df = spark.read.parquet(RAW_PATH)
    logger.info(f"Raw row count: {df.count():,}")
    return df




def clean(df):
    return (
        df
        # Cast types
        .withColumn("date",   F.to_date("date"))
        .withColumn("open",   F.col("open").cast("double"))
        .withColumn("high",   F.col("high").cast("double"))
        .withColumn("low",    F.col("low").cast("double"))
        .withColumn("close",  F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("long"))
        # Drop rows where close price is null
        .filter(F.col("close").isNotNull())
        # Deduplicate on natural key — handles overlap from 3-day lookback
        .dropDuplicates(["ticker", "date"])
        # Drop ingestion metadata columns not needed downstream
        .drop("ingestion_date")
    )


def enrich(df):
    """Add technical indicators and time features."""

    w_ticker  = Window.partitionBy("ticker").orderBy("date")
    w_30d     = Window.partitionBy("ticker").orderBy("date").rowsBetween(-29, 0)
    w_7d      = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)

    return (
        df
        # ── Daily return ──────────────────────────────────────
        .withColumn("prev_close", F.lag("close", 1).over(w_ticker))
        .withColumn("daily_return",
            (F.col("close") - F.col("prev_close")) / F.col("prev_close"))

        .withColumn("ma_7",  F.avg("close").over(w_7d))
        .withColumn("ma_30", F.avg("close").over(w_30d))

        .withColumn("volatility_30d",
            F.stddev("daily_return").over(w_30d))


        .drop("prev_close")
    )


def write_to_bigquery(df):
    logger.info(f"Writing to BigQuery table {BQ_TABLE}")
    (
        df.write
        .format("bigquery")
        .option("table", BQ_TABLE)
        .option("temporaryGcsBucket", GCS_BUCKET)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE") 
        .mode("overwrite") 
        .save()
    )
    logger.info("✅ Written to BigQuery successfully")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()

    try:
        df = read_raw(spark)
        df = clean(df)
        df = enrich(df)
        write_to_bigquery(df)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()