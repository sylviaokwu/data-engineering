
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion.load_raw_file import ingest_incremental_to_gcs
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="multi_asset_incremental_ingestion",
    description="Daily incremental load with 3-day lookback for all asset classes",
    start_date=datetime(2026, 4, 8),
    schedule_interval="0 2 * * *", 
    catchup=False,
) as dag:

    incremental_ingest = PythonOperator(
        task_id="ingest_incremental_to_gcs",
        python_callable=ingest_incremental_to_gcs,
        op_kwargs={
            "lookback_days": 3,
        },
    )

    process_assets = SparkSubmitOperator(
    task_id="process_assets_spark",
    application="/opt/airflow/spark_jobs/process_assets.py",
    conn_id="spark_default",
    jars="/opt/airflow/jars/gcs-connector-hadoop3-2.2.22.jar,/opt/airflow/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar",
    conf={
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopAbstractFileSystem",
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/opt/spark/secrets/gcp-key.json",
        "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/secrets/gcp-key.json",  # ← forces executor to use spark path
        "spark.yarn.appMasterEnv.GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/secrets/gcp-key.json",
    },
    
    dag=dag,
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt && dbt seed --target dev',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run --target dev',
    )

incremental_ingest >> process_assets >> dbt_seed>> dbt_run 