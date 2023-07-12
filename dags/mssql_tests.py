from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime



with DAG(
        dag_id='msssql_test',
        start_date=days_ago(2),
        schedule_interval=None,
        tags=['mssql']
) as dag:
    MsSqlOperator(
        task_id='test_mssql',
        sql=[
            "select 1 as a",
            "select 2 as b"
        ],
        dag=dag
    )