# dags/data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_ingest(**kwargs):
    import sys
    sys.path.insert(0, "/opt/airflow/dags/jobs")
    from load_raw_file import ingest_data_to_gcs
    ingest_data_to_gcs(days_back=365, interval="1d")

with DAG(
    dag_id="data_pipeline",
    start_date=datetime(2026, 1, 1),
    #schedule_interval="@daily",
    catchup=False,
    tags=["crypto", "gcs", "bigquery", "dbt"]
) as dag:

    ingest = PythonOperator(
        task_id="ingest_data_to_gcs",
        python_callable=run_ingest
    )

    ingest


    # Step 2 — Load GCS Parquet → BigQuery (via PySpark)
#    load_to_bq = BashOperator(
 #       task_id="load_to_bigquery",
  #      bash_command="spark-submit /opt/spark/jobs/load_crypto_bq.py"
#    )
#
    # Step 3 — Run dbt transformations
 #   dbt_run = BashOperator(
  #      task_id="dbt_run",
   #     bash_command=(
    #        "dbt run "
     #       "--project-dir /opt/airflow/dbt "
      #      "--profiles-dir /opt/airflow/dbt "
       #     "--select stg_prices crypto_prices mart_crypto_summary"
 #       )
  #  )


