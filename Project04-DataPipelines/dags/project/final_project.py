from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

"""
This Apache Airflow Directed Acyclic Graph (DAG) creates a pipeline to support Sparkify.

1)  Events and Song Play data from Sparkify is copied from AWS S3 to AWS Staging Tables using the StageToRedshiftOperator.
2)  After Staging Tables are loaded, the songplays fact table is loaded using the LoadFactOperator.
3)  After the Fact Table is loaded, the dimension tables are loaded using the LoadDimensionOperator.
4)  Once the Fact and Dimension Tables are loaded, the DataQualityOeprator is run to ensure that the tables are populated with records.

The DAG by default does not depend on past runs, will start running as of 2023-08-20, will retry 3 times upon failure, and will wait 5 minutes between retries.  Catchup is defaulted to False.  Email on Retry is defaulted to False.

"""


default_args = {
    'owner': 'mmendes',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='airflowlesson',
        s3_key='log-data',
        log_json_path= 's3://airflowlesson/log_json_path.json'  #replace with path to your log_josn_path meta data file.  This is required to copy event data correctly.
  
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='airflowlesson',
        s3_key='song-data/A/A/A', #limit song data to A/A/A per guidance on Udacity Knowlege Forum.  Full source is very large and can be expensive to run.
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.songplay_table_insert

    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.user_table_insert,
        write_mode='delete-load'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.song_table_insert,
        write_mode='delete-load'

    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.artist_table_insert,
        write_mode='delete-load'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.time_table_insert,
        write_mode='delete-load'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays','users','songs','artists','time']

    )

    end_operator = DummyOperator(task_id='End_execution')


    #DAG task dependencies

    start_operator >> stage_songs_to_redshift
    start_operator >> stage_events_to_redshift

    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()