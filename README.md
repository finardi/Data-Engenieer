# Data Modeling
> #### This project build an ETL pipeline with Postgres and Python in two datasets of music streaming app. The main goal of this solution is understanding what songs users are listening to. The ETL uses star schema with the following tables:

## Dimension Tables
*`users`* table features: user_id, first_name, last_name, gender, level


*`songs`* table features: song_id, title, artist_id, year, duration


*`artists`* table features: artist_id, name, location, latitude, longitude


*`time`* table features: start_time, hour, day, week, month, year, weekday


## Fact Table
*`songplays`* table features: songplay_id, start_time, user_d, level, song_id, artist_id, session_id, location, user_agent, see image below:

![song_plays table](song_plays.png)


## Pipeline description

The `etl.py` implements the pipeline with the follow functions

*`process_song_file`*: process and insert data into song_data and artist_data tables the arguments are:
- `cur` - PostgreSQL connection cursor 
- `filepath` -  json path that will be read with pandas data frame


*`process_log_file`*: process and insert data into time_table, user_table and song_plays:                
- cur - PostgreSQL connection cursor 
- filepath -  json path that will be read with pandas data frame


*`process_data`*: process process_song_file and process_log_file functions:                
- cur - PostgreSQL connection cursor 
- conn - PostgreSQL connection
- filepath -  json path that will be read with pandas data frame
- func - function to process data, the functions process_song_file and process_log_file


Also there is a etl.ipynb with the steps of each function in etl.py


## Usage
To build the ETL:
- run `create_tables.py` to create the database.
- run `etl.py` to perform the pipeline. 
- to see the results run the notebook `test.ipynb`