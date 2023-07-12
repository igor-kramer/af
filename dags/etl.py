from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import random


def fn_insert_click(ti):
    hook = ClickHouseHook()
    data=ti.xcom_pull(task_ids='generate_numbers')
    print(data)
    hook.run('INSERT INTO mytable VALUES', data)

def fn_insert_ms(ti):
    hook = MsSqlHook()
    data=ti.xcom_pull(task_ids='generate_numbers')
    hook.insert_rows('mytable', data)

def fn_insert_pg(ti):
    hook = PostgresHook()
    data=ti.xcom_pull(task_ids='generate_numbers')
    hook.insert_rows(table='mytable', rows=data, target_fields=['a'])

with DAG(
        dag_id='etl_dag',
        start_date=days_ago(2),
        schedule_interval=None,
        tags=['etl']
) as dag:
    
    start = DummyOperator(task_id='start_of_etl', dag=dag)
    middle = DummyOperator(task_id='middle_of_etl', dag=dag)
    finish = DummyOperator(task_id='finish_of_etl', dag=dag)

    create_table_pg = PostgresOperator(
        task_id='create_pg_table',
        sql='create table if not exists mytable(a integer)',
        dag=dag
    )

    create_table_ms = MsSqlOperator(
        task_id='create_ms_table',
        sql="IF OBJECT_ID(N'mytable', N'U') IS NULL create table mytable(a integer)",
        dag=dag
    )

    create_table_ch = ClickHouseOperator(
        task_id='create_ch_table',
        sql='create table if not exists mytable(a Int) engine=MergeTree() order by a',
        dag=dag
    )

    generate_data=PythonOperator(
        task_id='generate_numbers',
        python_callable=lambda: [[random.randint(1, 100000),] for _ in range(100)],
        dag=dag
    )

    insert_click = PythonOperator(
        task_id='insert_click',
        python_callable=fn_insert_click,
        dag=dag
    )

    insert_ms = PythonOperator(
        task_id='insert_ms',
        python_callable=fn_insert_ms,
        dag=dag
    )

    insert_pg = PythonOperator(
        task_id='insert_pg',
        python_callable=fn_insert_pg,
        dag=dag
    )

    start >> [create_table_ch, create_table_ms, create_table_pg] >> generate_data >> [insert_click, insert_ms, insert_pg] >> finish


