# DROP TABLES

# Using confidional drop table from postgres documentation
# https://www.postgresql.org/docs/8.2/sql-droptable.html
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# songplay_id and user_id as serial type to ensure that a null value cannot be inserted
# REF.: https://www.postgresql.org/docs/9.1/datatype-numeric.html
songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id bigserial PRIMARY KEY, start_time bigint, user_id bigserial, level varchar, \
                                         song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar) """)

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar) """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float) """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, artist_name varchar, location varchar, latitude float, longitude float) """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int) """)

# INSERT RECORDS
songplay_table_insert = (""" INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, \
                                                    artist_id, session_id, location, user_agent) \
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                                                    ON CONFLICT(songplay_id) DO NOTHING; """)

# Here, I literally following the instructions of the reviewr
user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) \
                                                    VALUES (%s, %s, %s, %s, %s) \
                                                    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level; """)

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) \
                                                    VALUES (%s, %s, %s, %s, %s) \
                                                    ON CONFLICT DO NOTHING; """)

artist_table_insert = (""" INSERT INTO artists (artist_id, artist_name, location, latitude, longitude) \
                                                    VALUES (%s, %s, %s, %s, %s) \
                                                    ON CONFLICT DO NOTHING; """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s) \
                                                    ON CONFLICT DO NOTHING; """)

# FIND SONGS
song_select = ("""SELECT songs.song_id, artists.artist_id FROM artists, songs \
                  WHERE songs.title = %s AND artists.artist_name=%s AND songs.duration=%s """) # idk if i need to use join clause here

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]