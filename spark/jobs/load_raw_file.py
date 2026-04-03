#!/usr/bin/env python3
"""
Crypto data ingestion from Yahoo Finance → GCS
Callable as a single function from Airflow DAG
"""

import yfinance as yf
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta
import os
import io
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CRYPTO_TICKERS = [
    "BTC-USD",
    "ETH-USD",
    "BNB-USD",
    "XRP-USD",
    "SOL-USD",
    "ADA-USD",
    "DOGE-USD",
    "AVAX-USD",
    "DOT-USD",
    "MATIC-USD",
]


def ingest_data_to_gcs(
    tickers: list = CRYPTO_TICKERS,
    bucket_name: str = None,
    gcs_prefix: str = "raw/prices",
    days_back: int = 365,
    interval: str = "1d"
):
    """
    Fetches crypto OHLCV data from Yahoo Finance and uploads
    as Parquet to GCS. Designed to be called from Airflow PythonOperator.
    """
    bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME", "terraform-sylvia-data")
    end_date   = datetime.today().strftime("%Y-%m-%d")
    start_date = "2018-01-01"
    # ── 1. Fetch from Yahoo Finance ──────────────────────
    logger.info(f"Fetching {len(tickers)} tickers | {start_date} → {end_date}")
    raw = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        interval=interval,
        group_by="ticker",
        auto_adjust=True,
        progress=False
    )

    frames = []
    for ticker in tickers:
        try:
            df = raw[ticker].copy()
            df["ticker"] = ticker
            df["ingestion_date"] = datetime.today().strftime("%Y-%m-%d")
            df.reset_index(inplace=True)
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            frames.append(df)
        except KeyError:
            logger.warning(f"No data for {ticker}, skipping.")

    if not frames:
        raise ValueError("No data fetched for any ticker.")

    result = pd.concat(frames, ignore_index=True)
    logger.info(f"Fetched {len(result)} rows across {len(frames)} tickers")

    # ── 2. Upload to GCS as Parquet ──────────────────────
    date_str  = datetime.today().strftime("%Y-%m-%d")
    blob_path = f"{gcs_prefix}/{date_str}_prices.parquet"

    buffer = io.BytesIO()
    result.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    logger.info(f"✅ Uploaded → gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"


if __name__ == "__main__":
    ingest_crypto_to_gcs()
