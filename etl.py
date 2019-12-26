import configparser
from datetime import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This method is used to create a spark session with necessary libraries included

    Args:

    Returns:
        An initialized spark session

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This method processes the song data and creates multiple tables: songs and artists

    Args:
        spark: a valid spark session containing the necessary libraries
        input_data: the base path to where to read the files from
        output_data: the base path of where to write the result files to

    Returns:
        None
    """
    # song data files are read from source folders
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    song_df = spark.read.json(song_data)

    # columns for creating the song_table are selected
    songs_table = song_df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()    
    songs_table.show(1)
    
    # songs table is written to parquet files into the given output folder
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_data + "songs.parquet")

    # columns for creating the artists table are selected and renamed
    artists_table = song_df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
                                    .withColumnRenamed("artist_location", "location") \
                                    .withColumnRenamed("artist_latitude", "latitude") \
                                    .withColumnRenamed("artist_longitude", "longitude")
    
    artists_table.show(1)
    
    # artists table is written to parquet files into the given output folder
    artists_table.write.mode("overwrite").parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    This method processes the log data and creates multiple tables: users, time and song_data

    Args:
        spark: a valid spark session containing the necessary libraries
        input_data: the base path to where to read the files from
        output_data: the base path of where to write the result files to

    Returns:
        None
    """
    # log data files are read from source folders
    log_df = spark.read.json(input_data + "log_data/2018/11/2018-11-08-events.json")
    
    # only actions for song plays are taken into account
    log_df = log_df.filter("page == 'NextSong'")

    # columns for creating the user table are selected and renamed
    users_table = log_df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    users_table = users_table.withColumnRenamed("userId", "user_id") \
                                .withColumnRenamed("firstName", "first_name") \
                                .withColumnRenamed("lastName", "last_name")
    
    users_table.show(1)
    
    # users table is written to parquet files in specified output folders
    users_table.write.mode("overwrite").parquet(output_data + "users.parquet")

    #  timestamp column is created from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    log_df = log_df.withColumn("new_timestamp", get_timestamp(log_df.ts))
        
    weekDay = F.udf(lambda x: x.weekday())
    
    # necessary columns for time table are created
    log_df = log_df.withColumn("hour", F.hour(log_df.new_timestamp)).withColumn("day", F.dayofmonth(log_df.new_timestamp))\
                    .withColumn("week", F.weekofyear(log_df.new_timestamp)).withColumn("month", F.month(log_df.new_timestamp))\
                    .withColumn("year", F.year(log_df.new_timestamp)).withColumn("weekday", weekDay(log_df.new_timestamp))
    
    # columns for creating time table are selected and renamed
    time_table = log_df.select("ts", "new_timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_table = time_table.withColumnRenamed("ts", "timestamp") \
                           .withColumnRenamed("new_timestamp", "datetime")
    time_table.show(1)
    
    # time table is written to parquet files in specified output folder
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "time.parquet")

    # song data is read in to be joined with log data
    songs_df = spark.read.json(input_data + "song-data/A/A/A/*.json")
    songs_df = songs_df.select("title", "artist_name", "song_id", "artist_id").dropDuplicates()
    
    songplays_df = log_df.join(songs_df, (log_df.song == songs_df.title) & (log_df.artist == songs_df.artist_name))
    
    # columns for creating the songplays table are selected and renamed
    songplays_table = songplays_df.select("ts", "year", "month", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent")
    songplays_table = songplays_table.withColumnRenamed("ts", "start_time") \
                                        .withColumnRenamed("userId", "user_id") \
                                        .withColumnRenamed("sessionId", "session_id") \
                                        .withColumnRenamed("userAgent", "user_agent")
    
    # a column for storing a unique identifier is created
    songplays_table = songplays_table.withColumn("songplay_id", F. monotonically_increasing_id())
    
    songplays_table.show(1)

    # songplays table is written to parquet files in the specified output folder
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "songplays.parquet")


def main():
    """
    This is the entry point of the data pipeline. It triggers the jobs to process the relevant data. 
    Args:
        None
    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pm-sparkify-bucket/"
    
    #input_data = "data/"
    #output_data = "data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
