
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'udacity',
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_failure': False
}

dag=DAG('create_table_dag',
         default_args=default_args,
          description='Create Tables in Redshift for Airflow',
          start_date=datetime(2018,11,1),
          catchup=False,
          schedule_interval='@hourly'
        )

create_sparkify_tables=PostgresOperator(
      dag=dag,
      task_id='create_sparkify_tables_id',
      postgres_conn_id='redshift',
      sql='create_tables.sql'
     )