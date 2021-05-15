import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
access_id=config.get('AWS', 'AWS_ACCESS_KEY_ID')
access_key=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
       Creates a session for the Spark SQL Application using Builder method of the SparkSession module.
       First config option downloads the Apache web module and executes the maven task to store its jar packages under local directory ~/ivy2
       Second config option defines the name of the Spark application
       getOrCreate() method create a session if it does not exist already
       spark context is a handle to the hadoop properties.
       Some of the hadoop properties have been modified to resolve the errors faced during executing the script.
    """   
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3")\
        .appName("Sparkify DataLake")\
        .getOrCreate()
    return spark
spark=create_spark_session()

sc=spark.sparkContext
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.block.size","32000000")
hadoop_conf.set("fs.s3a.multipart.size","104857600")
hadoop_conf.set("fs.s3a.threads.core","4")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)

def process_song_data(spark, input_data, output_data):
    """
     Reads json files for song_data from the s3 bucket udacity-dend, uses spark data frames to extract specific columns
     for defining the analytic tables, songs and artists, and write them to my bucket sudha-datalake in the parquet format. 
    """
    # initialize list of alphabets to iterate as folders for the song_data files
    list1=['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
    for folder1 in list1:
        song_data = os.path.join(input_data, 'song_data',folder1,'*','*','*.json')
        # read song data file
        df = spark.read.json(song_data)
        # create a temporary view to use in sql statements
        df.createOrReplaceTempView("song_data")
        # extract columns to create songs table
        songs_table = spark.sql("select song_id, title, artist_id, year, duration from song_data")
        # dropping duplicates
        songs_table =songs_table.dropDuplicates()
        # write songs table to parquet files partitioned by year and artist
        songs_table_output=songs_table.write.partitionBy("year", "artist_id").mode("append").parquet(output_data+"songs_parquet")
        # extract columns to create artists table
        artists_table = spark.sql("select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from song_data")
        # dropping duplicates
        artists_table = artists_table.dropDuplicates()
        # write artists table to parquet files
        artists_table_output=artists_table.write.mode("append").parquet(output_data+"artists_parquet")
                     
def process_log_data(spark, input_data, output_data):
    """
     Reads json files for log_data from the s3 bucket udacity-dend, uses spark data frames to extract specific columns
     for defining the analytic tables,users, time and songplays, and write them to my bucket sudha-datalake in the parquet format. 
    """
    
    # get filepath to log data file
    log_data =os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

   # create a temporary view to use in sql statements
    df.createOrReplaceTempView("log_data")
    
    # filter by actions for song plays
    df = spark.sql("select ts, userId, level, song, artist, sessionId, location, userAgent, length from log_data where page='NextSong'")

    # extract columns for users table    
    users_table = spark.sql("select userId, firstName, gender, lastName, level from log_data")

    # dropping duplicates
    users_table = users_table.dropDuplicates()
       
    # write users table to parquet files
    users_table_output=users_table.write.mode("append").parquet(output_data+"users_parquet")
 
     # create timestamp column from original timestamp column
    #get_timestamp = udf()
    get_timestamp = spark.sql("select from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss') as timestamp from log_data")
    #df = 

    # create datetime column from original timestamp column
    #get_datetime = udf()
    get_datetime=spark.sql("select cast(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss') as date) as datetime from log_data")
    #df = 

    # extract columns to create time table
    time_table = spark.sql("select from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss') as timestamp, hour(from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss')) as hour, dayofmonth(from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss')) as day, weekofyear(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as week, month(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as month, year(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as year, date_format(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'),'EEEE') as weekday from log_data")

    # dropping duplicates
    time_table = time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table_output= time_table.write.partitionBy("year", "month").mode("append").parquet(output_data+"time_parquet")

  # read in song data to use for songplays table
    song_df = spark.sql("select from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss')as start_time, userId, level, sessionId, location, userAgent from log_data")

    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("select from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss') as start_time, userId, level, song_id, artist_id, sessionId, location, userAgent, year(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as year, month(from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss')) as month from song_data \
    join log_data on trim(upper(artist))=trim(upper(artist_name)) and trim(upper(song))=trim(upper(title)) and length=duration where page='NextSong'")

    # dropping duplicates
    songplays_table = songplays_table.dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table_output=songplays_table.write.partitionBy("year", "month").mode("append").parquet(output_data+"songplays_parquet")


def main():
    """
    define the urls for s3 buckets being read and being written to by the above two functions.
    calls the above two functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake2/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
