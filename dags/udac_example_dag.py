from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    #according to guidelines
    'depends_on_past': False, #The DAG does not have dependencies on past runs
    'max_active_runs': 3, #On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), #retries happen every 5 minutes
    'catchup': False, #catchup is turned off
    'email_on_retry': False, #do not email on retries
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format="'s3://udacity-dend/log_json_path.json'",
    timeformat = "timeformat as 'epochmillisecs'",
    create_sql = SqlQueries.stagingevents_table_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_format="'auto'",
    timeformat = "",
    create_sql = SqlQueries.stagingsongs_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    create_sql = SqlQueries.songplays_table_create,
    insert_sql = SqlQueries.songplays_table_insert,
    append = False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="user",
    create_sql = SqlQueries.users_table_create,
    insert_sql = SqlQueries.users_table_insert,
    truncate = True,
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="song",
    create_sql = SqlQueries.songs_table_create,
    insert_sql = SqlQueries.songs_table_insert,
    truncate = True,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artist",
    create_sql = SqlQueries.artist_table_create,
    insert_sql = SqlQueries.artist_table_insert,
    truncate = True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    create_sql = SqlQueries.time_table_create,
    insert_sql = SqlQueries.time_table_insert,
    truncate = True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >>load_songplays_table
stage_songs_to_redshift >>load_songplays_table
load_songplays_table >>load_user_dimension_table
load_songplays_table >>load_song_dimension_table
load_songplays_table >>load_artist_dimension_table
load_songplays_table >>load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
