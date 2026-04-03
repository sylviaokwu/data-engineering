FROM apache/airflow:2.9.2-python3.11

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install -qqq \
    vim \
    openjdk-17-jdk-headless \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR $AIRFLOW_HOME

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER $AIRFLOW_UID