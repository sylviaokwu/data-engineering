# dags/crypto_historical_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion.load_raw_file import ingest_historical_to_gcs

with DAG(
    dag_id="multi_asset_historical_ingestion",
    description="One-time historical backfill from 2018 → today for all asset classes",
    start_date=datetime(2026, 4, 8),
    schedule_interval=None,      
    catchup=False,
) as dag:

    historical_ingest = PythonOperator(
        task_id="ingest_historical_to_gcs",
        python_callable=ingest_historical_to_gcs,
        op_kwargs={
            "start_date": "2018-01-01",
        },
    )