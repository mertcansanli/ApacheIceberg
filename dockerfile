FROM apache/airflow:3.0.6
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    pandas \
    apache-airflow-providers-apache-iceberg \
    apache-airflow-providers-trino