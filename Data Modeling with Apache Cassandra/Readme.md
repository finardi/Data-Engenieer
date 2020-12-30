# Data Modeling
> #### This project build an ETL pipeline for a music streaming app with Apache Cassandra and Python. There is a single dataset: `event_data` in CSV format. The directory of CSV files are partitioned by date. The ETL pipeline transfers data from the set of CSV files to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

The structure of this ETL pipeline follows:


*`event_data`* directory with CSV files partitioned by date.


*`images`* directory with single image of the streamlined CSV file.


*`cql_queries.py`* all create tables, insert datas and query to be used in Apache Cassandra.


*`dataprep.py`* a python class do transform the CSV file in the final processd format.

*`event_data_file_new.csv`* the CSV file processed bu `dataprep.py`.

*`Project_1B_ Project_Template.ipynb`* a detailed description step by step of the ETL pipeline solution.

*`etl.py`* functions to each query that connect in the Apache Cassandra, performs the ETL pipeline and disconnect and drop tables.


## Usage
To build the ETL:
- run `etl.py` this will build the pipeline and print the answers to the asked queries with the parameter argument `verbose=True`. The functions are flexible and it's possible to run with others parameters values in each function.


### Example
`python etl.py` will procude the output:

```
Query 1: Give me the artist, song title and song's length in the music app history that was heard during  sessionId=338, and itemInSession=4
Artist: Faithless, Song: Music Matters (Mark Knight Dub) Length: 495.30731

Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
Artist: Down To The Bone, Song: Keep On Keepin' On, User first name: Sylvie, User last name: Cruz
Artist: Three Drives, Song: Greece 2000, User first name: Sylvie, User last name: Cruz
Artist: Sebastien Tellier, Song: Kilometer, User first name: Sylvie, User last name: Cruz
Artist: Lonnie Gordon, Song: Catch You Baby (Steve Pitron & Max Sanna Radio Edit), User first name: Sylvie, User last name: Cruz

Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
User first name: Jacqueline, User last name: Lynch
User first name: Tegan, User last name: Levine
User first name: Sara, User last name: Johnson
        
```
