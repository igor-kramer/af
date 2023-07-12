from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime



with DAG(
        dag_id='postgres_operations',
        start_date=days_ago(2),
        schedule_interval='@hourly',
        tags=['pg']
) as dag:
    PostgresOperator(
        task_id='test_postgres',
        sql=[
            "select 1 as a",
            "select 2 as b"
        ],
        dag=dag
    )