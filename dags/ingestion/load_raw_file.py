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
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "SOL-USD", "ADA-USD", "DOGE-USD", "AVAX-USD", "DOT-USD", "MATIC-USD",
    "LTC-USD", "LINK-USD", "UNI-USD", "ATOM-USD", "XLM-USD","ALGO-USD", "TRX-USD", "ETC-USD", "NEAR-USD", "ARB-USD",
    "OP-USD", "INJ-USD", "SUI-USD", "FET-USD", "LDO-USD",]

STOCK_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA","JPM", "GS", "BAC", "V", "MA",
    "XOM", "CVX", "BP","JNJ", "PFE", "UNH","BRK-B", "WMT", "DIS",
]
ETF_TICKERS = [
    "SPY", "QQQ","VTI", "VWCE.DE","VGWL.AS", "GLD", "SLV", "TLT", "IEF", "VNQ",   
]
ALL_TICKERS = CRYPTO_TICKERS + STOCK_TICKERS + ETF_TICKERS 

ASSET_CLASS_MAP = (
    {t: "crypto"    for t in CRYPTO_TICKERS}
    | {t: "stock"     for t in STOCK_TICKERS}
    | {t: "etf"       for t in ETF_TICKERS}
)

def _fetch_tickers(tickers, start_date, end_date, interval):
    """Shared fetch logic used by both historical and incremental functions."""
    raw = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        interval=interval,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    frames, skipped = [], []
    for ticker in tickers:
        try:
            df = raw[ticker].copy()
            if df.empty or df.dropna(how="all").empty:
                skipped.append(ticker)
                continue
            df["ticker"]         = ticker
            df["asset_class"]    = ASSET_CLASS_MAP[ticker]
            df["ingestion_date"] = datetime.today().strftime("%Y-%m-%d")
            df.reset_index(inplace=True)
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            frames.append(df)
        except KeyError:
            skipped.append(ticker)

    if skipped:
        logger.warning(f"Skipped {len(skipped)} tickers: {skipped}")
    if not frames:
        raise ValueError("No data fetched for any ticker.")

    result = pd.concat(frames, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"], errors='coerce').dt.date
    result["ingestion_date"] = pd.to_datetime(result["ingestion_date"], errors='coerce').dt.date
    result['volume'] = result['volume'].astype('float64')
    logger.info(f"Fetched {len(result):,} rows | asset classes: {result['asset_class'].value_counts().to_dict()}")
    return result


def _upload_to_gcs(df, bucket_name, blob_path):
    """Shared GCS upload logic."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info(f"Uploaded → gs://{bucket_name}/{blob_path}")


def ingest_historical_to_gcs(
    tickers: list = ALL_TICKERS,
    bucket_name: str = None,
    gcs_prefix: str = "raw/prices",
    start_date: str = "2018-01-01",
    interval: str = "1d",
):
    """
    One-time full historical load. Writes one file per ticker-batch.
    Run this manually once to backfill.
    """
    bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME", "terraform-sylvia-data")
    end_date    = datetime.today().strftime("%Y-%m-%d")

    result = _fetch_tickers(tickers, start_date, end_date, interval)

    # Write as a single historical snapshot
    blob_path = f"{gcs_prefix}/historical_{end_date}.parquet"
    _upload_to_gcs(result, bucket_name, blob_path)

    logger.info(f"✅ Historical load done → gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"


def ingest_incremental_to_gcs(
    tickers: list = ALL_TICKERS,
    bucket_name: str = None,
    gcs_prefix: str = "raw/prices",
    lookback_days: int = 3,       # fetch last 3 days to handle weekends/gaps
    interval: str = "1d",
):
    """
    Daily incremental load. Fetches last N days to cover weekends/holidays.
    Spark handles deduplication at processing stage.
    Run this from Airflow on a daily schedule.
    """
    bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME", "terraform-sylvia-data")
    end_date    = datetime.today()
    start_date  = (end_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date    = end_date.strftime("%Y-%m-%d")

    result = _fetch_tickers(tickers, start_date, end_date, interval)

    # Each daily run gets its own file — no overwrites, easy to audit
    blob_path = f"{gcs_prefix}/{end_date}.parquet"
    _upload_to_gcs(result, bucket_name, blob_path)

    logger.info(f"✅ Incremental load done → gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"