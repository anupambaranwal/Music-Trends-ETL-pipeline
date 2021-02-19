import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def check_region_data(spark, datalake_bucket):
    region_df = spark.read.parquet(os.path.join(datalake_bucket, 'music_table/*.parquet'))

    if region_df.count() == 0:
        raise AssertionError('Music table is empty.')


def check_region_data(spark, datalake_bucket):
    region_df = spark.read.parquet(os.path.join(datalake_bucket, 'lyrics_table/*.parquet'))

    if region_df.count() == 0:
        raise AssertionError('Lyrics table is empty.')


def check_station_data(spark, datalake_bucket):
    station_df = spark.read.parquet(os.path.join(datalake_bucket, 'track_table/*.parquet'))

    if station_df.count() == 0:
        raise AssertionError('Track table is empty.')


def check_weather_data(spark, datalake_bucket):
    weather_df = spark.read.parquet(os.path.join(datalake_bucket, 'song_table/*.parquet'))

    if weather_df.count() == 0:
        raise AssertionError('Song table is empty.')

def check_region_data(spark, datalake_bucket):
    region_df = spark.read.parquet(os.path.join(datalake_bucket, 'artists_table/*.parquet'))

    if region_df.count() == 0:
        raise AssertionError('Artists table is empty.')

def check_region_data(spark, datalake_bucket):
    region_df = spark.read.parquet(os.path.join(datalake_bucket, 'features_table/*.parquet'))

    if region_df.count() == 0:
        raise AssertionError('Aeatures table is empty.')


def main():
    if len(sys.argv) == 2:
        # aws cluster mode
        datalake_bucket = sys.argv[1]
    else:
        # local mode
        config = configparser.ConfigParser()
        config.read('../dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        datalake_bucket = 's3a://' + config['S3']['BIKESHARE_DATALAKE_BUCKET'] + '/'

    spark = create_spark_session()

    check_trip_data(spark, datalake_bucket)
    check_time_data(spark, datalake_bucket)
    check_station_data(spark, datalake_bucket)
    check_weather_data(spark, datalake_bucket)
    check_region_data(spark, datalake_bucket)

if __name__ == "__main__":
    main()