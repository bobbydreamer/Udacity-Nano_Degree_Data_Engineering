'''
Run the below program in AWS EMR 

Pre-requisites
1. Upload merged_song_data.json, merged_log_data.json and etl-emrA.py to S3
2. Log into EMR Cluster Machine and download (1) from S3 and copy to HDFS filesystem

Run the below command in AWS EMR Cluster
   sudo /usr/bin/spark-submit --master yarn ./etl-emrA.py > etl-emrA.txt

Below are the ETL Steps,
1. Extract(Local)
    # INPUT : 
        input_data_song = "./merged_song_data.json"
        input_data_log = "./merged_log_data.json"

2. Transform(EMR)
3. Load
    # OUTPUT : 
        a) HDFS output_path = "./output2/"    
'''
import configparser
import os
from time import time
import numpy as np
import pandas as pd

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType, LongType, DoubleType

import datetime #Required for ts conversion
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

import glob
import json
import boto3

def upload_files(s3, bucket_name, folder, path):
    """
    Summary line. 
    Uploads files to S3
  
    Parameters: 
    arg1 : (S3 resource object)
    arg2 : (Bucket name)
    arg3 : (filename)
    arg4 : (local File path)
  
    Returns: None
    """        
    
    bucket = s3.Bucket(bucket_name)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                #print('Fullpath : ',full_path)
                #print(full_path[len(path)+1:])
                #print(folder+'/'+full_path[len(path)+1:])
                #bucket.upload_file('/tmp/' + filename, '<bucket-name>', 'folder/{}'.format(filename))
                bucket.put_object(Key=folder+'/'+full_path[len(path)+1:], Body=data)

def create_spark_session():
    """
    Summary line. 
    Create spark session
  
    Parameters: None
  
    Returns: 
    spark object
    """            
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("DataLake - ETL Local") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_path):
    """
    Summary line. 
    Process song data
  
    Parameters: 
    arg1 (spark object)
    arg2 (Read input from this path which can be local or S3)
  
    Returns: 
    song_df, songs_table, artists_table dataframes
    """    
    
    ps_start = time()
    print('Starting to process song data')
    
    # read song data file
    song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", FloatType()),
        StructField("artist_longitude", FloatType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", FloatType()),
        StructField("year", IntegerType())
    ])

    song_start = time()
    song_df = spark.read.json(input_data_path, 
              schema=song_schema)
    song_et = time()-song_start
    print("=== {} Total Elapsed time is {} sec\n".format('Read songs file', round(song_et,2) ))    

    # Replace artist_latitude & artist_longitude null with 0
    song_df = song_df.fillna({'artist_latitude':0})
    song_df = song_df.fillna({'artist_longitude':0})    
    
    # extract columns to create songs table
    songs_table = song_df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()    

    # extract columns to create artists table
    artists_table = song_df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
                    .withColumnRenamed('artist_name','name') \
                    .withColumnRenamed('artist_location','location') \
                    .withColumnRenamed('artist_latitude','latitude') \
                    .withColumnRenamed('artist_longitude','longitude').dropDuplicates()
        
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process songs file', round(ps_et,2) ))      
    return song_df, songs_table, artists_table


def process_log_data(spark, input_data_path):
    """
    Summary line. 
    Process log data
  
    Parameters: 
    arg1 (spark object)
    arg2 (Read input from this path which can be local or S3)
  
    Returns: 
    log_df, users_table, time_table, user_listen
    """    
    
    pl_start = time()        
    print('Starting to process log data')
    # get filepath to log data file
    log_data = input_data_path

    # read log data file
    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType()),
        StructField("status", StringType()),
        StructField("ts", StringType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])

    log_df = spark.read.json(input_data_path, 
              schema=log_schema)
    
    # Number of songs users listened to during each level
    paid_users = log_df.select(['userId', 'level']).filter( log_df['level'] == 'paid')
    paid_users = paid_users.groupby(['userId']).count()
    free_users = log_df.select(['userId', 'level']).filter( log_df['level'] == 'free')
    free_users = free_users.groupby(['userId']).count()
    paid_users.createOrReplaceTempView('paid_users')
    free_users.createOrReplaceTempView('free_users')
    user_listen = spark.sql("""
        select a.userId, a.count puCount, b.count fuCount
        from paid_users a join free_users b
        on a.userId = b.userId
        where a.userId != ''
    """)        
    
    # Filter only column page with value "NextSong"
    log_df = log_df.filter(log_df.page == 'NextSong').collect()
    
    # Convert List to Spark
    log_df = spark.createDataFrame(log_df,schema=log_schema) 
    
    # Convert ts from long to datetime
    convert_ts = udf(lambda x: datetime.datetime.fromtimestamp(float(x) / 1000.0), TimestampType())
    log_df = log_df.withColumn("ts_converted", convert_ts(log_df.ts))
    
    # Convert registration from double to long
    log_df = log_df.withColumn("registration_converted", log_df.registration.cast(LongType()) )

    pl_et = time() - pl_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process log files : Read & Transformation', round(pl_et,2) ))       
    
    print('Creating users table')
    temp_start = time()        
    # extract columns for users table    
    # creating users table with columns user_id, first_name, last_name, gender, level
    users_table = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])\
            .withColumnRenamed('userId', 'user_id')\
            .withColumnRenamed('firstName', 'first_name')\
            .withColumnRenamed('lastName', 'last_name').dropDuplicates()

    pl_et = time() - temp_start
    print("=== {} Total Elapsed time is {} sec\n".format('Creating users table', round(pl_et,2) ))       

    print('Creating user_listen table')
    temp_start = time()
    user_listen.createOrReplaceTempView('user_listen')
    users_table.createOrReplaceTempView('users')    
    user_listen = spark.sql("""
    select distinct b.first_name, a.puCount, a.fuCount
    from user_listen a join users b
    on a.userId = b.user_id
    """)
    pl_et = time() - temp_start
    print("=== {} Total Elapsed time is {} sec\n".format('Creating user_listen table', round(pl_et,2) ))

    # extract columns to create time table
    # Creating time table with columns start_time, hour, day, week, month, year, weekday
    print('Creating time table')
    temp_start = time()            
    time_table = log_df.select(['ts_converted'])\
                        .withColumnRenamed('ts_converted','start_time') 

    time_table = time_table.withColumn('day', F.dayofmonth('start_time')) \
                          .withColumn('month', F.month('start_time')) \
                          .withColumn('year', F.year('start_time')) \
                          .withColumn('hour', F.hour('start_time')) \
                          .withColumn('minute', F.minute('start_time')) \
                          .withColumn('second', F.second('start_time')) \
                          .withColumn('week', F.weekofyear('start_time')) \
                          .withColumn('weekday', F.dayofweek('start_time')).dropDuplicates()    
    pl_et = time() - temp_start
    print("=== {} Total Elapsed time is {} sec\n".format('Creating time table', round(pl_et,2) ))       
    
    pl_et = time() - pl_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process log files : Total', round(pl_et,2) ))       
    return log_df, users_table, time_table, user_listen


def process_songplays(spark, log_df, songs_table, artists_table, users_table, time_table):
    """
    Summary line. 
    Process songplays
  
    Parameters: 
    arg1 (spark object)
    arg2 (log dataframe)
    arg3 (songs dataframe)
    arg4 (artists dataframe)
    arg5 (users dataframe)
    arg6 (time dataframe)
  
    Returns: 
    songplays_table
    """    
    
    psp_start = time()        
    print('Processing songplays')        
    
    # Creating tables
    songs_table.createOrReplaceTempView('songs')
    users_table.createOrReplaceTempView('users')
    artists_table.createOrReplaceTempView('artists')
    time_table.createOrReplaceTempView('time')
    log_df.createOrReplaceTempView('log_clean')
    
    songplays_table = spark.sql("""
    select  t.start_time, l.userId as user_id, l.level, s.song_id, a.artist_id, l.sessionId as session_id, l.location, l.userAgent as user_agent
    , t.year, t.month
    from log_clean l join time t
    on l.ts_converted = t.start_time
    join artists a
    on l.artist = a.name
    join songs s
    on l.song = s.title
    """)

    # create a monotonically increasing id 
    # A column that generates monotonically increasing 64-bit integers.
    # The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    songplays_table = songplays_table.withColumn("idx", F.monotonically_increasing_id())

    # Then since the id is increasing but not consecutive, it means you can sort by it, so you can use the `row_number`
    songplays_table.createOrReplaceTempView('songplays_table')
    songplays_table = spark.sql("""
    select row_number() over (order by "idx") as num, 
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month
    from songplays_table
    """)
    songplays_table.createOrReplaceTempView('songplays')
    psp_et = time() - psp_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process songplays', round(psp_et,2) ))    
    return songplays_table            


def write_parquet_files(spark, song_df, log_df, songs_table, artists_table, users_table, time_table, songplays_table, user_listen, output_path, bucket_name, location, s3c, s3r):
    """
    Summary line. 
    Write files first locally then uploads the folders to S3
  
    Parameters: 
    arg1 (spark object)
    arg2 (song dataframe)
    arg3 (log dataframe)
    arg4 (songs dataframe)
    arg5 (artists dataframe)
    arg6 (users dataframe)
    arg7 (time dataframe)
    arg8 (songplays dataframe)
    arg9 (user_listen dataframe)
    arg10 (output path contains S3)
    arg11 (bucketname)
    arg12 (location)
    arg13 (s3 Client Object)
    arg14 (s3 Resource Object)
  
    Returns: None    
    """    
    
    print('Show spark sql table row counts')
    songs_table.createOrReplaceTempView('songs')
    artists_table.createOrReplaceTempView('artists')    
    users_table.createOrReplaceTempView('users')    
    time_table.createOrReplaceTempView('time')
    songplays_table.createOrReplaceTempView('songplays')
    
    table_row_counts = spark.sql("""
    select 'users' as Tables, count(*) as Rows from users
    union all
    select 'songs', count(*) from songs
    union all
    select 'artists', count(*) from artists
    union all
    select 'time', count(*) from time
    union all
    select 'songplays', count(*) from songplays
    """)
    table_row_counts.show(10)

    print('Creating s3 bucket {}'.format(bucket_name))
    try:
        s3c.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
        print('-- Bucket Created')   
        #Below code not working. AWS came with a brilliant idea of making buckets public by default. 
        #bucket = s3r.bucket(bucket_name)
        #bucket.Acl().put(ACL='private')        
    except:
        print('-- Bucket Exists')   
    
    wp_start = time()        
    print('Starting to write parquet files')
    #output_path=output_path+''+bucket_name
    print('Output path {}'.format(output_path))
    
    # write songs table to parquet files partitioned by year and artist
    # Requirement is to partition by year & artist, since songs_table contains artist_id, so instead of artist_name, artist_id is used.
    songs_table.write.parquet(output_path+'/'+'songs.parquet', mode='overwrite', partitionBy=['year', 'artist_id'])    
    
    # write artists table to parquet files
    artists_table.write.parquet(output_path+'/'+'artists.parquet', mode='overwrite')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_path+'/'+'time.parquet', mode='overwrite', partitionBy=['year', 'month'])    

    # write users table to parquet files
    users_table.write.parquet(output_path+'/'+'users.parquet', mode='overwrite')        
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_path+'/'+'songplays.parquet', mode='overwrite', partitionBy=['year', 'month'])
    
    print('Starting to write CSV files')
    log_df.write.save(output_path+'/'+'log_clean.csv', mode='overwrite', format="csv", header=True)
    song_df.write.save(output_path+'/'+'song_clean.csv', mode='overwrite', format="csv", header=True)
    user_listen.write.save(output_path+'/'+'user_listen.csv', mode='overwrite', format="csv", header=True)    
    
    '''
    print('Starting upload to s3')
    tables = ['artists']
    tables = ['artists.parquet', 'songplays.parquet', 'songs.parquet', 'time.parquet', 'users.parquet', \
              'log_clean.csv', 'song_clean.csv', 'user_listen.csv']

    for table in tables:
        print('Uploading {} files'.format(table))
        t0 = time()
        filepath = output_path+'/'+table
        s3_subfolder = table
        s3c.put_object(Bucket=bucket_name, Key=(s3_subfolder+'/'))    
        
        s3_subfolder = bucket_name+'/'+s3_subfolder
        bucket = s3r.Bucket(s3_subfolder)
        upload_files(s3r, bucket_name, table, filepath)
        
        uploadTime = time()-t0
        print("=== {} Uploaded in: {} sec\n".format(table, round(uploadTime,2) ))

    '''
    wp_et = time() - wp_start
    print("=== {} Total Elapsed time is {} sec\n".format('Write parquet files', round(wp_et,2) ))       
        
    
    
def main():   
    spark = create_spark_session()
        
    input_data_song = "./song_data/*/*/*/*.json" #Works. But takes time
    input_data_song = "./merged_song_data.json"  #Since all the data is merged. Read is fast
            
    input_data_log = "./log_data/*/*/*.json" #Works. But takes time
    input_data_log = "./merged_log_data.json" #Since all the data is merged. Read is fast
    
    output_path = "./output2"

    bucket_name = 'sushanth-dend-datalake-parquet-files2'
    location = {'LocationConstraint': 'us-west-2'}    
    s3c = boto3.client('s3', region_name="us-west-2")
    s3r = boto3.resource('s3', region_name="us-west-2")

    total_start = time()
    
    song_df, songs_table, artists_table = process_song_data(spark, input_data_song)
    songs_table.show(5)
    artists_table.show(5)
    
    log_df, users_table, time_table, user_listen = process_log_data(spark, input_data_log)
    log_df.show(5)
    users_table.show(5)    
    time_table.show(5)
    user_listen.show(5)
    
    songplays_table = process_songplays(spark, log_df, songs_table, artists_table, users_table, time_table)
    
    write_parquet_files(spark, song_df, log_df, songs_table, artists_table \
                        , users_table, time_table, songplays_table, user_listen \
                        , output_path, bucket_name, location, s3c, s3r)


    total_et = time() - total_start
    print("=== {} Total Elapsed time is {} sec\n".format('Main()', round(total_et,2) ))    
    
    print('Done!')

if __name__ == "__main__":
    main()