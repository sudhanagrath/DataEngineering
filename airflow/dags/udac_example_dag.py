from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_failure': False
}
    
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2018,11,1),
          #end_date=datetime(2018,11,30,0,0,0,0),
          #max_active_runs=1,
          catchup=False,
          #schedule_interval='0 * * * *'
          schedule_interval='@hourly'
        )
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    schema='public',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{{ execution_date.replace(year=2018,month=11).year }}/{{ execution_date.replace(year=2018,month=11).month}}/',
    provide_context=True,
    region='us-west-2',
    json_path="s3://udacity-dend/log_json_path.json",
    use_partitioned_data=True
  )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    schema='public',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    provide_context=True,
    region='us-west-2'
)   

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    columns_list='(songplay_id,start_time,userid,level,songid,artistid,sessionid,location,user_agent)',
    insert_string=SqlQueries.songplay_table_insert,
    truncate_table=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    columns_list= '(userid,first_name, last_name, gender, "level")',
    insert_string=SqlQueries.user_table_insert,
    append=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    columns_list='(songid, title, artistid, year, duration)',
    insert_string=SqlQueries.song_table_insert,
    append=True
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    columns_list='(artistid, name, location, lattitude, longitude)',
    insert_string=SqlQueries.artist_table_insert,
    append=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    columns_list='(start_time, hour, day, week, month, year, weekday)',
    insert_string=SqlQueries.time_table_insert,
    append=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
      {'check_sql': 'select count(*) from users where userid is null', 'expected_result': 0},
      {'check_sql': 'select count(*) from songs where songid is null', 'expected_result': 0},
      {'check_sql': 'select count(*) from artists where artistid is null', 'expected_result': 0},
      {'check_sql': 'select count(*) from time where start_time is null', 'expected_result': 0}
    ]
  )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task Dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_user_dimension_table 
stage_songs_to_redshift >> load_song_dimension_table 
stage_songs_to_redshift >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table 
[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table ] >> run_quality_checks >> end_operator  
