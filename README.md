# Data Engineering Pipeline

End-to-end data engineering project using **Airflow**, **Spark**, **dbt**, and **GCS/BigQuery** on Google Cloud Platform. Designed for easy reproducibility using GitHub Codespaces.

This project builds a fully automated, containerised data pipeline for ingestingand transforming multi-asset financial market data — with a focus on crypto prices.
It was built to solve a real problem: taking raw market data delivered daily via an external API, making it reliable, queryable, and ready for analysis .

The pipeline handles the full journey from raw API ingestion through to structured BigQuery tables that dbt models build on. 

## Architecture

```
GCS (raw parquet)
    ↓  Airflow DAG (daily @ 2am)
Spark (transform + enrich)
    ↓
BigQuery staging
    ↓  dbt
BigQuery warehouse → mart
```


![alt text](flow.png) 

## Problem Statement

Financial market data (specifically cryptocurrency prices) is often delivered as raw, high-velocity JSON or CSV files from disparate APIs. This presents several challenges for data analysts:

- **Manual Overhead**: Downloading and cleaning data daily is time-consuming and prone to human error.
- **Inconsistency**: Raw API outputs lack the structure required for complex historical analysis or cross-asset comparisons.
- **Data Gaps**: Traditional ingestion often misses late-arriving data or fails during API downtime.
- **Infrastructure Complexity**: Manually setting up cloud buckets, databases, and processing clusters is difficult to reproduce and manage.

**This project solves these problems** by providing a fully automated, containerized pipeline that handles everything from infrastructure provisioning (Terraform) to production-grade SQL modeling (dbt), ensuring data is always reliable, queryable, and ready for analysis.

## Tech Stack

- **Orchestration**: Apache Airflow 
- **Processing**: Apache Spark (PySpark)
- **Transformation**: dbt-bigquery 
- **Storage**: Google Cloud Storage
- **Warehouse**: BigQuery
- **Infrastructure**: Terraform
- **Environment**: Docker Compose + GitHub Codespaces

---

## Prerequisites

- Google Cloud Platform account with a project created
- GitHub account (for Codespaces)
- **Docker Desktop** (if running locally on Mac/Windows)

---

## Setup Instructions

### 1. Initial Setup

Run the setup script to create the required directory structure (including the `secrets/` folder) and generate your `.env` file:

```bash
chmod +x init-setup.sh
./init-setup.sh
```

---

### 2. Configure Credentials

1.  **GCP Key**: Place your Google Cloud service account key inside the `secrets/` folder (created in step 1) and rename it to `gcp-key.json`.
2.  **Environment Variables**: Open the `.env` file and fill in your `GCP_PROJECT_ID` and `GCS_BUCKET` name.

---

### 3. Start the Pipeline

Everything else is automated! Just run:

```bash
docker-compose up -d
```

> [!TIP]
> If you are on a Mac, ensure **Docker Desktop** is open and the status is "Running" before executing this command.

This will automatically:
- Provision your GCS bucket and BigQuery dataset via **Terraform**.
- Initialize the **Airflow** metadata database.
- Start **Airflow** (Webserver & Scheduler) and **Spark**.
- Pre-configure **dbt** with your environment settings.

---

### 4. Access the Interfaces

- **Airflow UI**: `http://localhost:8080` (login: `admin` / `admin`)
- **Spark UI**: `http://localhost:8090`

---

### 5. Trigger the pipeline

Go to the Airflow UI, find the DAG `multi_asset_incremental_ingestion`, and trigger it manually. The pipeline will handle ingestion, Spark processing, and dbt transformations automatically.

---

## Project Structure

```
data-engineering/
├── .env                        # environment variables (gitignored)
├── docker-compose.yaml
├── README.md
├── init-setup.sh               # initial setup script
├── secrets/
│   └── gcp-key.json            # service account key (gitignored)
├── terraform/
│   ├── main.tf
│   └── variable.tf
├── docker/
│   ├── airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── spark/
│       └── Dockerfile
├── dags/
│   └── multi_asset_incremental_ingestion.py
├── spark/
│   └── jobs/
│       └── process_crypto.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
├── logs/                       # gitignored
└── plugins/
```
The dashboard was built using the data model. You can access the dashboard [here](https://datastudio.google.com/reporting/bf23e7d6-a7bc-4aa2-8203-2bdcce49035e)

![alt text](image.png)