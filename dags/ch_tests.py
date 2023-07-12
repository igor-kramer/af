from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime



with DAG(
        dag_id='clickhouse_test',
        start_date=days_ago(2),
        schedule_interval=None,
        tags=['clickhouse']
) as dag:
    ClickHouseOperator(
        task_id='test_clickhouse',
        sql=[
            "select 1 as a",
            "select 2 as b"
        ],
        dag=dag
    )