# Data Modeling
> #### Project 1 nano degree Data Enginieer - Udacity

This project build an ETL pipeline with Postgres and Python in two datsets of music streaming app. The main goal of this solution is understanding what songs users are listening to. The ETL uses star schema with the following tables:

> ### Dimension Tables
#### `users` - users in the app, features: user_id, first_name, last_name, gender, level

#### `songs` - songs in music database, features: song_id, title, artist_id, year, duration

#### `artists` - artists in music database, features: artist_id, name, location, latitude, longitude

#### `time` - timestamps of records in songplays, features: start_time, hour, day, week, month, year, weekday

> ### Fact Table
#### `songplays` - records in log data associated with song plays, features: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Usage
First run `create_tables.py` to create the database, then run ```etl.py``` to perform the pipeline. The  `sql_queries.py` contains all the SQL queries used in the solution, `etl.ipynb` contains in details the ETL solution and `test.ipynb` is a notebook to see the results of the ETL pipeline. 

