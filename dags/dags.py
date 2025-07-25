from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from plugins.stage_loader import *
from plugins.data_check import *
from sql_queries import *
from datetime import datetime, timedelta
default_args = {
    'owner' : 'tune_stream',
    'depends_on_past' : False,
    'start_date' : datetime(2025, 7, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'catchup': False,
    'email_on_retry': False
}

with DAG('final_etl_pipeline',
         default_args=default_args,
         description='ETL pipeline',
         schedule='@hourly',
         catchup=False) as dag:

    start_execution = EmptyOperator(task_id='begin_execution')

    stage_songs = PythonOperator(
        task_id='Stage_songs',
        python_callable=load_songs_to_staging
    )

    stage_events = PythonOperator(
        task_id='Stage_events',
        python_callable=load_events_to_staging
    )

    load_songplays_fact_table = SQLExecuteQueryOperator(
        task_id='Load_songplays_fact_table',
        conn_id='postgres',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dim_table = SQLExecuteQueryOperator(
        task_id='Load_user_dim_table',
        conn_id='postgres',
        sql=SqlQueries.user_table_insert
    )

    load_song_dim_table = SQLExecuteQueryOperator(
        task_id='Load_song_dim_table',
        conn_id='postgres',
        sql=SqlQueries.song_table_insert
    )

    load_artist_dim_table = SQLExecuteQueryOperator(
        task_id='Load_artist_dim_table',
        conn_id='postgres',
        sql=SqlQueries.artist_table_insert
    )

    load_time_dim_table = SQLExecuteQueryOperator(
        task_id='Load_time_dim_table',
        conn_id='postgres',
        sql=SqlQueries.time_table_insert
    )

    run_data_quality_checks = PythonOperator(
        task_id='data_quality_checks',
        python_callable=check_table_records
    )

    end_execution = EmptyOperator(task_id='End_execution')


    start_execution >> [stage_songs, stage_events] >> load_songplays_fact_table
    load_songplays_fact_table >> [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table]
    [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table] >> run_data_quality_checks
    run_data_quality_checks >> end_execution
