from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Mahmoud Anwer',
    'start_date': datetime(2019, 1, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG("Data Pipelines",
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table="staging_events",
    s3_bucket = 'udacity-dend',
    s3_key = "log_data",
    s3_path = "s3://udacity-dend/log_data",
    file_format="s3://udacity-dend/log_json_path.json",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)



stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    s3_bucket = 'udacity-dend',
    s3_key = "song_data",
    s3_path = "s3://udacity-dend/song_data/A/A/A",
    file_format="auto",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert,
    truncate=True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    redshift_conn_id="redshift",
    truncate=True,
    sql_query = SqlQueries.user_table_insert
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = "songs",
    redshift_conn_id="redshift",
    truncate=True,
    sql_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = "artists",
    redshift_conn_id="redshift",
    truncate=True,
    sql_query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = "time",
    redshift_conn_id="redshift",
    truncate=True,
    sql_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    #tables=[ 'staging_songs', 'staging_events','songplays','users','time', 'artists', 'songs']
    checks=[
        {
            "table": "songplays",
            "test_sql": "SELECT COUNT(*) FROM songplays",
            "expected_result": 0,
        },
        {
            "table": "songs",
            "test_sql": "SELECT COUNT(*) FROM songs",
            "expected_result": 0,
        },
        {
            "table": "artists",
            "test_sql": "SELECT COUNT(*) FROM artists",
            "expected_result": 0,
        },
        {
            "table": "users",
            "test_sql": "SELECT COUNT(*) FROM users",
            "expected_result": 0,
        },
        {
            "table": "time",
            "test_sql": 'SELECT COUNT(*) FROM time',
            "expected_result": 0,
        }
    ]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator



