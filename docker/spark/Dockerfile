FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    curl && \
    pip install --upgrade pip && \
    rm -rf /var/lib/apt/lists/*

ARG GCS_CONNECTOR_VERSION=hadoop3-2.2.22
RUN curl -fsSL \
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-${GCS_CONNECTOR_VERSION}.jar" \
    -o /opt/spark/jars/gcs-connector-${GCS_CONNECTOR_VERSION}.jar

ARG BQ_CONNECTOR_VERSION=0.36.1
RUN curl -fsSL \
    "https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/${BQ_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar" \
    -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar

RUN pip install --no-cache-dir \
    google-cloud-storage==2.16.0 \
    google-cloud-bigquery==3.21.0 \
    pandas==2.0.3 \
    pyarrow==12.0.1

USER 185