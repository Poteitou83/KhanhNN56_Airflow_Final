from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG('create_tables_dag',
         start_date=datetime(2025,7,20),
         schedule=None,
         catchup=False) as dag:
    
    create_tables = SQLExecuteQueryOperator(
        task_id = 'create_tables',
        conn_id = 'postgres',
        sql = 'create_tables.sql'
    )