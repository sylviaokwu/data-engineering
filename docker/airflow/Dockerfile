FROM apache/airflow:2.9.2-python3.11

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install -qqq \
    vim \
    openjdk-17-jdk-headless \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download GCS connector jar so spark-submit can reference it
RUN mkdir -p /opt/airflow/jars && \
    curl -fsSL \
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22.jar" \
    -o /opt/airflow/jars/gcs-connector-hadoop3-2.2.22.jar

WORKDIR $AIRFLOW_HOME

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER $AIRFLOW_UID