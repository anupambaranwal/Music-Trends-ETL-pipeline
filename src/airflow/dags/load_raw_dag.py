from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFileS3Operator, CheckS3FileCount

default_args = {
    'owner': 'anupam',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

spark_script_bucket_name = 'anupam-spark-script'
raw_datalake_bucket_name = 'anupam-raw-datalake'
processed_datalake_bucket_name = 'anupam-processed-datalake'

dag = DAG('load_raw_dag',
          default_args=default_args,
          description='Create and load data into S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_datalake = CreateS3BucketOperator(
    task_id='Create_raw_datalake',
    bucket_name=raw_datalake_bucket_name,
    dag=dag
)

upload_playlist_data = UploadFileS3Operator(
	task_id='upload_playlist_data',
	bucket_name=raw_datalake_bucket_name,
	path='/home/workspace/airflow/datasets/playlist_data',
	dag=dag
	)

upload_genre_data = UploadFileS3Operator(
	task_id='upload_genre_data',
	bucket_name=raw_datalake_bucket_name,
	path='/home/workspace/airflow/datasets/genre_data',
	dag=dag
	)

upload_lyrics_data = UploadFileS3Operator(
	task_id='upload_lyrics_data',
	bucket_name=raw_datalake_bucket_name,
	path='/home/workspace/airflow/datasets/lyrics_data',
	dag=dag
	)

upload_charts_data = UploadFileS3Operator(
	task_id='upload_charts_data',
	bucket_name=raw_datalake_bucket_name,
	path='/home/workspace/airflow/datasets/charts_data',
	dag=dag
	)

upload_artists_data = UploadFileS3Operator(
	task_id='upload_artists_data',
	bucket_name=raw_datalake_bucket_name,
	path='/home/workspace/airflow/datasets/artists_data',
	dag=dag
	)

check_data_quality = CheckS3FileCount(
    task_id='check_data_quality',
    bucket_name=raw_datalake_bucket_name,
    expected_count=5,
    dag=dag
)

create_code_bucket = CreateS3BucketOperator(
    task_id='Create_code_bucket',
    bucket_name=spark_script_bucket_name,
    dag=dag
)

create_datalake_bucket = CreateS3BucketOperator(
    task_id='Create_datalake_bucket',
    bucket_name=processed_datalake_bucket_name,
    dag=dag
)

upload_etl_code = UploadFileS3Operator(
    task_id='Upload_etl_code',
    bucket_name=spark_script_bucket_name,
    path='/home/workspace/script/',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define dependency
start_operator >> create_raw_datalake

create_raw_datalake >> upload_artists_data
create_raw_datalake >> upload_charts_data
create_raw_datalake >> upload_genre_data
create_raw_datalake >> upload_lyrics_data
create_raw_datalake >> upload_playlist_data

upload_artists_data >> check_data_quality
upload_charts_data >> check_data_quality
upload_genre_data >> check_data_quality
upload_lyrics_data >> check_data_quality
upload_playlist_data >> check_data_quality

check_data_quality >> create_code_bucket
create_code_bucket >> upload_etl_code
check_data_quality >> create_datalake_bucket

upload_etl_code >> end_operator
create_datalake_bucket >> end_operator