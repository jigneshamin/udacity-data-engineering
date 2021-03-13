import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id, from_unixtime
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates Spark session and returns the session. 
    
    Params: 
    - None
    
    Returns:
    - Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song_data from S3 and loads data in 
    songs and artists table
    
    Params:
    - spark: Spark session
    - input_data: File path to source file bucket
    - output_data: Path to destination bucket
    
    Returns:
    - None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*')
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_fields = ["title","artist_id","year","duration"]
    songs_table = df.select(songs_fields).dropDuplicates(subset=['song_id']).withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_fields = ["artist_id","artist_name as name","artist_locatin as location","artist_latitude as latitude","artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")

def process_log_data(spark, input_data, output_data):
    """
    Processes log_data from S3 and loads data in 
    users, time, and songplays table
    
    Params:
    - spark: Spark session
    - input_data: File path to source file bucket
    - output_data: Path to destination bucket
    
    Returns:
    - None
    """  
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates(subset=['user_id'])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    time_table = df.select("start_time","hour","day","week","month","year")
    time_table = time_table.drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    song_logs = df.join(song_df, (df.song == song_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_logs = song_logs.join(artists_df, (song_logs.artist == artists_df.name))
    
    songplays = artists_songs_logs.join(time_table, artists_songs_logs.ts == time_table.ts, 'left').drop(artists_songs_logs.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month')
    ).repartition("year","month")

    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "songplays")
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-adkfs/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
