from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

"""
This file creates the DAG and provides the task dependencies
to start the airflow server please run "/opt/airflow/start.sh" in the terminal
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    #'start_date': datetime(2022, 1, 1),
    'depends_on_past':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'email_on_retry': False,
}

quality_checks = [
    {'name': 'check nulls in songplays',
    'sql_query':'SELECT COUNT(*) FROM songplays WHERE playid is null',
    'expected_result': 0
    },
    {'name': 'check nulls in artists',
    'sql_query':'SELECT COUNT(*) FROM songs WHERE artistid is null',
    'expected_result': 0
    },
    {'name': 'check nulls in users',
    'sql_query':'SELECT COUNT(*) FROM songs WHERE userid is null',
    'expected_result': 0
    },
    {'name': 'check nulls in songs',
    'sql_query':'SELECT COUNT(*) FROM songs WHERE songid is null',
    'expected_result': 0
    }
]


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='@monthly'
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/C/"    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    sql_statement=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    sql_statement=SqlQueries.user_table_insert,
    redshift_conn_id="redshift",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    sql_statement=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    sql_statement=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    sql_statement=SqlQueries.time_table_insert,
    redshift_conn_id="redshift",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    checks=quality_checks,
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# set task dependencies
start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
