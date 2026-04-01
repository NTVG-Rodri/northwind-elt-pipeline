from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


def read_sql():
    with open('/opt/airflow/data/northwind.sql', 'r') as f:
        return f.read()

with DAG(
    dag_id='dag_load_northwind',
    start_date=datetime(2024,1,1),
    schedule="@once",
    catchup=False,
) as dag:
    
    load = PostgresOperator(
        task_id='load_northwind_sql',
        postgres_conn_id='postgres-dwh',
        sql=read_sql(),
    )