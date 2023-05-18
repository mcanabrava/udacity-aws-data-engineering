from datetime import datetime, timedelta

import pendulum
import os

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimensions import LoadDimensionOperator
from data_quality import DataQualityOperator
from sql_queries import SqlQueries

default_args = {
    "owner": "marcelo",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    start_date=pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    end_date=pendulum.datetime(2018, 11, 30, 0, 0, 0, 0),
    description="Load and transform data in Redshift with Airflow",
    schedule_interval='@hourly',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="load_stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-airflow-project-marcelo",
        s3_key="log-data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}-events.json",
        jsonpath="log_json_path.json",
        table_name="public.staging_events"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="load_stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-airflow-project-marcelo",
        s3_key="song-data/A/A/A/",
        table_name="public.staging_songs"
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.user_table_insert,
        table="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.song_table_insert,
        table="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.artist_table_insert,
        table="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.time_table_insert,
        table="time",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tables=["artists", "songplays", "songs", "time", "users"],
    )

    end_operator = DummyOperator(task_id="End_execution")

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    (
    load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    >> run_quality_checks   
    >> end_operator
    )


final_project_dag = final_project()