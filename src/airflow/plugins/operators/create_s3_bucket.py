from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
import boto3
from botocore.exceptions import ClientError

class CreateS3BucketOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_conn_id='aws_credentials',
                 bucket_name='',
                 region_name='us-west-2',
                 *args, **kwargs):
        super(CreateS3BucketOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.region_name = region_name

    def execute(self, context):
        try:
            s3_hook = S3Hook(self.aws_conn_id)
            s3_hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)
            self.log.info(f'Created {self.bucket_name} bucket in {self.region_name} region.')
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print("Bucket already created")
            else:
                print("Unexpected error: %s" % e)

