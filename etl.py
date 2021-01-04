import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    description: Loads the dataset song_data from S3 and extract specific data: Song and artist dimension tables
                (using spark dataframes) to upload the data to S3 (the output data lake) in parquet format.

    :param spark: spark session
    :param input_data: location of song_data json files with the songs metadata
    :param output_data: location of the s3 bucket (output data lake) where the dimension table will be stored
    """
    # get filepath to song data file
    song_data =  os.path.join(input_data, "song_data/*/*/*/*.json")
    #print(song_data)
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 
                     'title', 
                     'artist_id',
                     'year', 
                     'duration'
                    ].dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("------>songs.parquet is completed")

    # extract columns to create artists table
    artists_table = df['artist_id', 
                       'artist_name', 
                       'artist_location', 
                       'artist_latitude', 
                       'artist_longitude'].dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("------>artists.parquet is completed")


def process_log_data(spark, input_data, output_data):
    """
    description: Loads the dataset song_data from S3 and extract specific data: users  and time dimension tables as
                well as songplays  fact table (using spark dataframes) to upload the data to S3 (the output data lake)
                in parquet format.

    :param spark: spark session
    :param input_data: location of song_data json files with the songs metadata
    :param output_data: location of the s3 bucket (output data lake) where the dimension and fact tables will be stored
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")
    print(log_data)
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # extract columns for users table    
    users_table  = df['userId', 'firstName', 'lastName', 'gender', 'level'].dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("------>users.parquet is completed")

    # create timestamp column from original timestamp column
    get_timestamp =  udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime =udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                            col('datetime').alias('start_time'),
                            hour('datetime').alias('hour'),
                            dayofmonth('datetime').alias('day'),
                            weekofyear('datetime').alias('week'),
                            month('datetime').alias('month'),
                            year('datetime').alias('year') 
                       ).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("------>time.parquet is completed")

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json")).dropDuplicates(['song_id'])

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.title == df.song
                              & song_df.artist_name == df.artist
                              & song_df.duration == df.length,
                              'inner'
                              )

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.select(
                                                monotonically_increasing_id().alias('songplay_id'),
                                                col('datetime').alias('start_time'),
                                                col('userId').alias('user_id'),
                                                col('level').alias('level'),
                                                col('song_id').alias('song_id'),
                                                col('artist_id').alias('artist_id'),
                                                col('sessionId').alias('session_id'),
                                                col('location').alias('location'),
                                                col('userAgent').alias('user_agent'),
                                                year('datetime').alias('year'),
                                                month('datetime').alias('month')
                                            )
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("------>songplays.parquet is completed")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" #replace with your personal s3 bucket
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
