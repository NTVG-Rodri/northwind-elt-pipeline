FROM apache/airflow:2.8.0
USER root
RUN apt-get update && apt-get install -y git
USER airflow
RUN pip install apache-airflow-providers-postgres \
    dbt-postgres