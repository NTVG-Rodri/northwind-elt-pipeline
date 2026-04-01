from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='dag_dbt_transform',
    start_date=datetime(2024,1,1),
    schedule='@daily',
    catchup=False,
) as dag:
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='/home/airflow/.local/bin/dbt run --project-dir /opt/airflow/dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='/home/airflow/.local/bin/dbt test --project-dir /opt/airflow/dbt'
    )
    
    dbt_run >> dbt_test