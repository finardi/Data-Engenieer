import configparser

#=================
# CONFIG
#=================
config = configparser.ConfigParser()
config.read('config.cfg')

IAM_ROLE = config.get("SET_CONFIG","ARN")
LOG_DATA = config.get("S3_CONFIG","LOG_DATA")
LOG_JSONPATH = config.get("S3_CONFIG", "LOG_JSONPATH")
SONG_DATA = config.get("S3_CONFIG", "SONG_DATA")

#=================
# DROP TABLES
#=================
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

#=================
# CREATE TABLES
#=================
# registration fails with type int, changed to bigint
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table(artist varchar, auth varchar, 
                                                firstName varchar, gender varchar, itemInSession integer,
                                                lastName varchar, length float, level varchar, location varchar,
                                                method varchar, page varchar, registration bigint, sessionId integer,
                                                song varchar, status integer, ts timestamp, userAgent varchar, userId integer);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table(song_id varchar, num_songs integer, title varchar,
                                                artist_name varchar, artist_latitude float, year integer, duration float,
                                                artist_id varchar, artist_longitude float, artist_location varchar);""")

# songplay_id fails with only type integer found that RedShift needs to use IDENTITY, here 
# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS fact_songplays(songplay_id integer IDENTITY(0,1) PRIMARY KEY sortkey,
                                                start_time timestamp, user_id integer, level varchar, song_id varchar, artist_id varchar,
                                                session_id integer, location varchar, user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_users(user_id integer PRIMARY KEY distkey, first_name varchar, 
                                                last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs(song_id varchar PRIMARY KEY, title varchar, artist_id varchar distkey,
                                                year integer, duration float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artists(artist_id varchar PRIMARY KEY distkey, name varchar,
                                                location varchar, latitude float, longitude float);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time(start_time timestamp PRIMARY KEY sortkey distkey, hour integer, day integer,
                                                week integer, month integer, year integer, weekday integer);""")


#=================
# STAGING TABLES
#=================
staging_events_copy = ("""COPY staging_events_table FROM {} CREDENTIALS 'aws_iam_role={}' COMPUPDATE OFF region 'us-west-2'
                                                TIMEFORMAT AS 'epochmillisecs' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                                                FORMAT AS JSON {};""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs_table FROM {} CREDENTIALS 'aws_iam_role={}' COMPUPDATE OFF region 'us-west-2'
                                                FORMAT AS JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                                                """).format(SONG_DATA, IAM_ROLE)


#=================
# FINAL TABLES
#=================

user_table_insert = ("""INSERT INTO dim_users(user_id, first_name, last_name, gender, level) SELECT DISTINCT userId AS user_id,
                                                firstName AS first_name, lastName AS last_name, gender AS gender, level AS level
                                                FROM staging_events_table WHERE userId IS NOT NULL;""")

# song_id always != NULL 
song_table_insert = ("""INSERT INTO dim_songs(song_id, title, artist_id, year, duration) SELECT DISTINCT song_id AS song_id,
                                                title AS title, artist_id AS artist_id, year AS year, duration AS duration
                                                FROM staging_songs_table WHERE song_id IS NOT NULL;""")

# artist_id always != NULL 
artist_table_insert = ("""INSERT INTO dim_artists(artist_id, name, location, latitude, longitude) SELECT DISTINCT artist_id AS artist_id,
                                                artist_name AS name, artist_location AS location, artist_latitude AS latitude, 
                                                artist_longitude AS longitude FROM staging_songs_table WHERE artist_id IS NOT NULL;""")

# ts always != NULL, in project1 we got ddmmyyy with pandas
time_table_insert = ("""INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday) SELECT distinct ts, EXTRACT(hour from ts),
                                                EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts),
                                                EXTRACT(weekday from ts) FROM staging_events_table WHERE ts IS NOT NULL;""")

# eg.: to_char(current_timestamp, 'FMDay, FMDD  HH12:MI:SS') 'Tuesday, 6  05:39:18', see
# documentation about to_timestamp https://www.postgresql.org/docs/10/functions-formatting.html

songplay_table_insert = ("""INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, 
                                                location, user_agent) SELECT DISTINCT staging_events.ts, staging_events.userId AS user_id, 
                                                staging_events.level AS level, staging_songs.song_id AS song_id,
                                                staging_songs.artist_id AS artist_id, staging_events.sessionId AS session_id, 
                                                staging_events.location AS location, staging_events.userAgent AS user_agent 
                                                FROM staging_events_table staging_events JOIN staging_songs_table staging_songs 
                                                ON staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name;""")


#=================
# QUERY LISTS
#=================
create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    songplay_table_create, 
    user_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create,
]

drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop,
]

copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy,
]

insert_table_queries = [
    songplay_table_insert, 
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert,
]