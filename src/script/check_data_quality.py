import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import DateType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def check_music_data(spark, datalake_bucket):
    music_df = spark.read.parquet(os.path.join(datalake_bucket, 'music_table/*.parquet'))

    if music_df.count() == 0:
        raise AssertionError('Music table is empty.')

    if music_df.where(col("track_id").isNull()):
        raise AssertionError('Primary key cannot be null.')


def check_lyrics_data(spark, datalake_bucket):
    lyrics_df = spark.read.parquet(os.path.join(datalake_bucket, 'lyrics_table/*.parquet'))

    if lyrics_df.count() == 0:
        raise AssertionError('Lyrics table is empty.')

    if lyrics_df.select(F.countDistinct("track_name")) != lyrics_df.select(F.count("track_name")):
        raise AssertionError('Primary key should be unique.')


def check_track_data(spark, datalake_bucket):
    track_df = spark.read.parquet(os.path.join(datalake_bucket, 'track_table/*.parquet'))

    if track_df.count() == 0:
        raise AssertionError('Track table is empty.')

    if dict(track_df.dtypes)[count_words] != 'int':
        raise AssertionError('Data type mis-match.')


def check_song_data(spark, datalake_bucket):
    song_df = spark.read.parquet(os.path.join(datalake_bucket, 'song_table/*.parquet'))

    if song_df.count() == 0:
        raise AssertionError('Song table is empty.')

def check_artists_data(spark, datalake_bucket):
    artists_df = spark.read.parquet(os.path.join(datalake_bucket, 'artists_table/*.parquet'))

    if artists_df.count() == 0:
        raise AssertionError('Artists table is empty.')

def check_features_data(spark, datalake_bucket):
    features_df = spark.read.parquet(os.path.join(datalake_bucket, 'features_table/*.parquet'))

    if features_df.count() == 0:
        raise AssertionError('Aeatures table is empty.')


def main():
    if len(sys.argv) == 2:
        datalake_bucket = sys.argv[1]
    else:
        config = configparser.ConfigParser()
        config.read('../dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        datalake_bucket = 's3a://' + config['S3']['BIKESHARE_DATALAKE_BUCKET'] + '/'

    spark = create_spark_session()

    check_music_data(spark, datalake_bucket)
    check_lyrics_data(spark, datalake_bucket)
    check_track_data(spark, datalake_bucket)
    check_song_data(spark, datalake_bucket)
    check_features_data(spark, datalake_bucket)

if __name__ == "__main__":
    main()