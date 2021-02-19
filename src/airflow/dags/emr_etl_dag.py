import airflow
import configparser
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import CreateS3BucketOperator, UploadFileS3Operator

spark_script_bucket_name = 'anupam-spark-script'
raw_datalake_bucket_name = 'anupam-raw-datalake'
processed_datalake_bucket_name = 'anupam-processed-datalake'

default_args = {
    'owner': 'anupam',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

SPARK_ETL_STEPS = [
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'Setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + spark_script_bucket_name, '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'Playlist - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/process_playlist_data.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + processed_datalake_bucket_name]
        }
    },
    {
        'Name': 'Genre - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/process_genre_data.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + processed_datalake_bucket_name]
        }
    },
    {
        'Name': 'Lyrics - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/process_lyrics_data.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + processed_datalake_bucket_name]
        }
    },
    {
        'Name': 'Charts - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/process_charts_data.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + processed_datalake_bucket_name]
        }
    },
    {
        'Name': 'Artists - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/process_artists_data.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + processed_datalake_bucket_name]
        }
    },
    {
        'Name': 'Check data quality',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/check_data_quality.py',
                     's3a://' + processed_datalake_bucket_name]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Datalake-ETL'
}

dag = DAG('emr_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    region_name='us-west-2',
    dag=dag
)

add_jobflow_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=SPARK_ETL_STEPS,
    dag=dag
)

playlist_processing = EmrStepSensor(
    task_id='playlist_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    dag=dag
    )

genre_processing = EmrStepSensor(
    task_id='genre_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[3] }}",
    aws_conn_id='aws_credentials',
    dag=dag
    )

lyrics_processing = EmrStepSensor(
    task_id='lyrics_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[4] }}",
    aws_conn_id='aws_credentials',
    dag=dag
    )

charts_processing = EmrStepSensor(
    task_id='charts_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[5] }}",
    aws_conn_id='aws_credentials',
    dag=dag
    )

artists_processing = EmrStepSensor(
    task_id='artists_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[6] }}",
    aws_conn_id='aws_credentials',
    dag=dag
    )


data_quality_check = EmrStepSensor(
    task_id='data_quality_check_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[7] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define dependency
start_operator >> create_cluster

create_cluster >> add_jobflow_steps

add_jobflow_steps >> playlist_processing
add_jobflow_steps >> genre_processing
add_jobflow_steps >> lyrics_processing
add_jobflow_steps >> charts_processing
add_jobflow_steps >> artists_processing

playlist_processing >> data_quality_check
genre_processing >> data_quality_check
lyrics_processing >> data_quality_check
charts_processing >> data_quality_check
artists_processing >> data_quality_check

data_quality_check >> end_operator


