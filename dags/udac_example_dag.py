
"""
    
    This DAG is responsible for running the all the tasks in the ETL data pipeline of Sparkify

"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3, 
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5), 
    'catchup': False
}

with DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          ,tags=["Airflow_ELT_Redshift"],
          schedule_interval='@hourly'

        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    # load from S3 to redshift stage events area
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        params={
            'table':"staging_events"
            ,'redshift_conn_id':'redshift'
            ,'s3_bucket':"udacity-dend"
            ,'s3_key':"log_data"
            ,'s3_json':"s3://udacity-dend/log_json_path.json"
            ,'aws_credentials_id':"aws_credentials"
        }
    )

    # load from S3 to redshift stage songs area
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        params={
            'table':"staging_songs"
            ,'redshift_conn_id':'redshift'
            ,'s3_bucket':"udacity-dend"
            ,'s3_key':"song_data"
            ,'s3_json':"auto"
            ,'aws_credentials_id':"aws_credentials"
        }
    )

    # load from stage area to fact table in redshift
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        params={
            'table':"songplays"
            ,'sql': SqlQueries.songplay_table_insert
            ,'redshift_conn_id':"redshift"
        }
    )

    # load from stage area to dimension table users in redshift
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag
        ,params={
            'table':"users"
            ,'sql': SqlQueries.user_table_insert
            ,'redshift_conn_id':"redshift"
            
        }

    )
    
    # load from stage area to dimension table songs in redshift
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag
        ,params={
            'table':"songs"
            ,'sql': SqlQueries.song_table_insert
            ,'redshift_conn_id':"redshift"
            
        }
    )

    # load from stage area to dimension table artists in redshift
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag
        ,params={
            'table':"artists"
            ,'sql': SqlQueries.artist_table_insert
            ,'redshift_conn_id':"redshift"
            
        }
    )

    # load from stage area to dimension time artists in redshift
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag
        ,params={
            'table':"time"
            ,'sql': SqlQueries.time_table_insert
            ,'redshift_conn_id':"redshift"
            
        }
    )
    
    with TaskGroup(group_id='Run_data_quality_checks') as run_quality_checks:
        # data quality checks

        #check values from artists dimension table in redshift
        data_quality_checks_artists = DataQualityOperator(
            task_id='data_quality_checks_artists',
            dag=dag,
            params={
                'conn_id':'redshift'
                ,'table':"artists"
                ,'col_name':"artist_id"
            }
    
            
        )
        
        #check values from songplays dimension table in redshift
        data_quality_checks_songplays = DataQualityOperator(
            task_id='data_quality_checks_songplays',
            dag=dag,
            params={
                'conn_id':'redshift'
                 ,'table': "songplays"
                 ,'col_name':"playid"
            }
            
        )

        #check values from songs dimension table in redshift
        data_quality_checks_songs = DataQualityOperator(
            task_id='data_quality_checks_songs',
            dag=dag,
            params={
                'conn_id':'redshift'
                ,'table': "songs"
                ,'col_name':"songid"
            }
            
        )

        #check values from time dimension table in redshift
        data_quality_checks_time = DataQualityOperator(
            task_id='data_quality_checks_time',
            dag=dag,
            params={
                'conn_id':'redshift'
                ,'table': "time"
                ,'col_name':"start_time"
            }
            
        )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_song_dimension_table] >> run_quality_checks >> end_operator