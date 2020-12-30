import os
import configparser
from datetime import datetime
from queries import *
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.functions import (weekofyear, date_format, month, 
                                   dayofmonth, hour, year)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('SIGN_AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('SIGN_AWS','AWS_SECRET_ACCESS_KEY')

@dataclass
class ETLClass:
    """
    A class used to perform an ETL with Spark
    ...

    Attributes
    ----------
    spark : object
        a pyspark session
    song_path : str
        a path to json files content infortmations about songs and artists 
    log_path : str
        a path to json files content informations about users, time and songs played
    output_data_path : str
        a path to store the processed files

    Methods
    -------
    prepare_data_song
        create a SQL table to extract columns from song json files and write this table
        in parquet format partitioned by year and artist
    prepare_data_log
        create a SQL table to extract columns from joined song and log tables and write 
        this table in parquet format partitioned by year and artist        
    """

    spark: object
    song_path: str
    log_path: str
    output_data_path: str

    def prepare_data_song(self, song_query_str, artists_query_str):
        """
        Extract, write and save song json files in parquet format partitioned by year and artist
        ...
      
        Attributes
        ----------
        song_query_str : str
            a query to perform song features from SONG_TABLE
        artists_query_str : str
            a query to perform artist features from SONG_TABLE

        """
      
        data = self.spark.read.json(self.song_path)
        data.createOrReplaceTempView("SONG_TABLE")

        song_query = self.spark.sql(song_query_str)
        song_query.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(self.output_data_path,"song_query/"))

        artists_query = self.spark.sql(artists_query_str)
        artists_query.write.mode("overwrite").parquet(os.path.join(self.output_data_path,"artists_query/"))

    def prepare_data_log(self, users_query_str, time_query_str, songplay_query_str):
        """
        Extract, write and save joined song and log tables in parquet format 
        partitioned by year and artist
        ...
      
        Attributes
        ----------
        users_query_str : str
            a query to perform users features from LOG_TABLE
        time_query_str : str
            a query to perform timestamp features from LOG_TABLE
        songplay_query_str : str
            a query to perform the join between song and log tables

        """
      
        data = self.spark.read.json(self.log_path)
        data = data.filter(data.page == "NextSong")
        data.createOrReplaceTempView("LOG_TABLE")

        users_query = self.spark.sql(users_query_str)
        users_query.write.mode("overwrite").parquet(os.path.join(self.output_data_path,"users_query/"))

        time_query = self.spark.sql(time_query_str)
        time_query.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(self.output_data_path,"time_query/"))

        songplay_query = self.spark.sql(songplay_query_str)
        songplay_query.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(self.output_data_path,"songplay_query/"))   

# Path files in S3
in_path  = "s3a://udacity-dend/"
out_path = "s3://spark-p4/outputs/"

# Datasets path files
song_path = os.path.join(in_path, "song_data", "*/*/*/*.json")
log_path = os.path.join(in_path, "log_data", "*/*/*.json") 

# Init spark session
spark_sess = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        
# Instantiate ETL class
ETL = ETLClass(
    spark=spark_sess,
    song_path=song_path,
    log_path=log_path, 
    output_data_path=out_path,
    )

# Process the ETL
ETL.prepare_data_song(song_query_str, artists_query_str)
ETL.prepare_data_log(users_query_str, time_query_str, songplay_query_str)        