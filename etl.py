import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window


config = configparser.ConfigParser()
config.read("dl.config")

os.environ["AWS_ACCESS_KEY_ID"]=config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Gets or creates a Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads the file `song_data` from a public S3 bucket to create
    the tables `artists` and `songs` and save them to the specified
    S3 location.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (df
        .select("song_id", "title", "artist_id", "year", "duration")
    )
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table
        .write
        .mode("overwrite")
        .partitionBy("year", "artist_id")
        .parquet(os.path.join(output_data, "songs"))
    )

    # extract columns to create artists table
    artists_table = (df
        .select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
        .withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
        .dropDuplicates(["artist_id"])
    )
    
    # write artists table to parquet files
    (artists_table
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_data, "artists"))
    )


def process_log_data(spark, input_data, output_data):
    """
    Reads the file `log_data` from a public S3 bucket to create
    the tables `songplays`, `time` and `users` and save them to
    the specified S3 location.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(F.col("page")=="NextSong")

    # extract columns for users table
    latest_record_per_user = (df.groupBy(F.col("userId")).agg(F.max(F.col("ts")).alias("ts")))  
    users_table = (df
        .join(latest_record_per_user, ["userId", "ts"], how="inner")
        .select("userId", "firstName", "lastName", "gender", "level")
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
    )
    
    # write users table to parquet files
    (users_table
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_data, "users"))
    )

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.fromtimestamp(ts/1000), T.TimestampType())
    df = df.withColumn("start_time", get_timestamp(F.col("ts")))

    # extract columns to create time table
    time_table = (df
        .select(
            "start_time",
            F.hour("start_time").alias("hour"),
            F.dayofmonth("start_time").alias("day"),
            F.weekofyear("start_time").alias("week"),
            F.month("start_time").alias("month"),
            F.year("start_time").alias("year"),
            F.dayofweek("start_time").alias("weekday")
        )
        .dropDuplicates(["start_time"])
    )
    
    # write time table to parquet files partitioned by year and month
    (time_table
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(os.path.join(output_data, "time"))
    )

    # read in song and artist data to use for songplays table
    artist_df = spark.read.parquet("s3a://sparkify-17sd2e9/artists")
    song_df = (spark.read.parquet("s3a://sparkify-17sd2e9/songs")
        .join(artist_df, "artist_id", how="left")
        .select("song_id", "artist_id", "title", "duration", "name")
        .withColumn("duration_rounded", F.round(F.col("duration"), 2))
    )

    # add songplay_id, extract columns from joined song and log datasets to create songplays table 
    window = Window.orderBy(F.col("start_time"), F.col("userId"))
    df = (df
        .withColumn("length_rounded", F.round(F.col("length"),2))
        .withColumn("songplay_id", F.row_number().over(window))
    )
    songplays_table = (df
        .join(song_df,
            (df.song == song_df.title) &
            (df.length_rounded == song_df.duration_rounded) &
            (df.artist == song_df.name),
            how="left")
        .select(
            "songplay_id",
            "start_time",
            F.col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            F.col("sessionId").alias("session_id"),
            "location",
            F.col("userAgent").alias("user_agent"),
            F.year("start_time").alias("year"),
            F.month("start_time").alias("month")
        )
    )

    # write songplays table to parquet files partitioned by year and month
    (songplays_table
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(os.path.join(output_data, "songplays"))
    )


def main():
    """
    Creates a spark session to process song_data
    and log_data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-17sd2e9"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
