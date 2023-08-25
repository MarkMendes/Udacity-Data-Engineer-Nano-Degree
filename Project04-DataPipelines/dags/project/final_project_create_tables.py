import pendulum
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator
import pendulum
import logging
from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from udacity.common.final_project_create_tables_sql import SqlQueries

default_args = {
    'owner': 'mmendes',
    'start_date' : pendulum.now(),
    'depends_on_past': False,
    'retries' : 0,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Create Redshift Tables',
    schedule_interval='@once'

)

def final_project_create_tables():
    start_operator = DummyOperator(task_id='Begin_execution')

    @task
    def load_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")

    create_stg_events_task=PostgresOperator(
        task_id="create_stg_events_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.staging_events_table_create
    )

    create_stg_songs_task=PostgresOperator(
        task_id="create_stg_songs_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.staging_songs_table_create
    )

    create_songplay_task=PostgresOperator(
        task_id="create_songplay_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.songplay_table_create
    )

    create_user_task=PostgresOperator(
        task_id="create_user_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.user_table_create
    )

    create_song_task=PostgresOperator(
        task_id="create_song_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.song_table_create
    )

    create_artist_task=PostgresOperator(
        task_id="create_artist_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.artist_table_create

    )

    create_time_task=PostgresOperator(
        task_id="create_time_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.time_table_create

    )          

final_project_create_tables_dag = final_project_create_tables()




