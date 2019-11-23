# Command to start Airflow Webserver
# /opt/airflow/start.sh

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from load_dim_subdag import load_dim_table_dag

from helpers import SqlQueries
from helpers import CreateTables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# DAG configuration
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'catchup':False,
}
start_date = datetime.utcnow()

parent_task_id = 'P6_Airflow_Pipeline_dag'
dag = DAG(parent_task_id,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Creating Staging tables
create_staging_events = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.create_staging_events
)

create_songs_table = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.create_staging_songs
)

# Staging the data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

# Create fact table : songplays
create_songplays = PostgresOperator(
    task_id="create_songplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.create_songplays
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

# Create & Load dimension tables
# artists table
load_artists_dimension_table_task_id = "CL_artists_subdag"
load_artists_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_task_id,
        load_artists_dimension_table_task_id,
        "redshift",
        "artists",
        CreateTables.create_artists,
        SqlQueries.artist_table_insert,        
        start_date=start_date
    ),
    task_id=load_artists_dimension_table_task_id,
    dag=dag,
)

#load_artist_dimension_table = LoadDimensionOperator(
#    task_id='Load_artist_dim_table',
#    dag=dag
#)

# songs table
load_songs_dimension_table_task_id = "CL_songs_subdag"
load_songs_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_task_id,
        load_songs_dimension_table_task_id,
        "redshift",
        "songs",
        CreateTables.create_songs,
        SqlQueries.song_table_insert,        
        start_date=start_date
    ),
    task_id=load_songs_dimension_table_task_id,
    dag=dag,
)

#load_song_dimension_table = LoadDimensionOperator(
#    task_id='Load_song_dim_table',
#    dag=dag
#)

# users table
load_users_dimension_table_task_id = "CL_users_subdag"
load_users_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_task_id,
        load_users_dimension_table_task_id,
        "redshift",
        "users",
        CreateTables.create_users,
        SqlQueries.user_table_insert,        
        start_date=start_date
    ),
    task_id=load_users_dimension_table_task_id,
    dag=dag,
)

#load_users_dimension_table = LoadDimensionOperator(
#    task_id='Load_users_dim_table',
#    dag=dag
#)

# time table
load_time_dimension_table_task_id = "CL_time_subdag"
load_time_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_task_id,
        load_time_dimension_table_task_id,
        "redshift",
        "time",
        CreateTables.create_time,
        SqlQueries.time_table_insert,        
        start_date=start_date
    ),
    task_id=load_time_dimension_table_task_id,
    dag=dag,
)

#load_time_dimension_table = LoadDimensionOperator(
#    task_id='Load_time_dim_table',
#    dag=dag
#)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_staging_events
start_operator >> create_songs_table

create_staging_events >> stage_events_to_redshift
create_songs_table >> stage_songs_to_redshift

stage_events_to_redshift >> create_songplays
stage_songs_to_redshift >> create_songplays

create_songplays >> load_songplays_table

load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table

load_users_dimension_table >> run_quality_checks
load_songs_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator