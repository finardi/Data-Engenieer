import os
import csv
import glob
import json
from cql_queries import *
from dataprep import DataPrep
from cassandra.cluster import Cluster

def connect_db():
    """Coonect and create a keyspace and cluster in Cassandra

    Returns
    -------
    session
        the Cassandra session to execute ETL
    cluster
        Cassandra node cluster
    """

    cluster = Cluster()

    session = cluster.connect()

    session.execute(
        """CREATE KEYSPACE IF NOT EXISTS p2_udacity 
           WITH REPLICATION = {'class': 'SimpleStrategy', 
                               'replication_factor': 1}"""
                   )
    session.set_keyspace('p2_udacity')
    
    return session, cluster

    
def execute_query_1(session, file, sessionId=338, itemInSession=4, verbose=True):
    """Give the artist, song title and song's length filtered by 
       sessionId and itemInSession.

    Parameters
    ----------
    session
        the Cassandra session to execute ETL
    file
        path to csv file
    sessionId
        a filter in the query
    itemInSession
        a filter in the query
    verbose
        prints and validate the query
    """

    session.execute(song_features)

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            itemInSession_, sessionId_ = int(line[3]), int(line[8])
            artist, song, length = str(line[0]), str(line[9]), float(line[5])
            session.execute(insert_data_song_features, (itemInSession_, sessionId_, artist, song, length))

    if verbose:
        print("\nQuery 1: Give me the artist, song title and song's length in the music app\
 history that was heard during  sessionId=338, and itemInSession=4")
        rows = session.execute(query_1, (itemInSession, sessionId))
        for row in rows:
            print(f'\tartist: {row.artist}, song: {row.song} length: {row.length:.8}')
            
def execute_query_2(session, file, userId=10, sessionId=182, verbose=True):
    """Give the name of artist, song and user (first and last name) 
       filtered by userid and sessionid

    Parameters
    ----------
    session
        the Cassandra session to execute ETL
    file
        path to csv file
    userId
        a filter in the query
    sessionId
        a filter in the query
    verbose
        prints and validate the query
    """

    session.execute(artist_song_by_user)

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            userId_, sessionId_, itemInSession  = int(line[10]), int(line[8]), int(line[3])
            artist, song, firstName, lastName = str(line[0]), str(line[9]), str(line[1]), str(line[4])
            session.execute(insert_data_artist_song_by_user, (userId_, sessionId_, itemInSession, artist, song, firstName, lastName))

    if verbose:
        print("\nQuery 2: Give me only the following: name of artist, song (sorted by itemInSession)\
 and user (first and last name) for userid = 10, sessionid = 182")
        rows = session.execute(query_2, (userId, sessionId))
        for row in rows:
            print(f'\tartist: {row.artist}, song: {row.song}, user first name: {row.firstname}, user last name: {row.lastname}')        

def execute_query_3(session, file, song='All Hands Against His Own', verbose=True):
    """Give the every user name (first and last) filtered by the song 'All Hands Against His Own

    Parameters
    ----------
    session
        the Cassandra session to execute ETL
    file
        path to csv file
    song
        a filter in the query
    verbose
        prints and validate the query
    """
    session.execute(user_name)

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            song, userId, = str(line[9]), int(line[10])
            firstName, lastName = str(line[1]), str(line[4])
            session.execute(insert_data_user_name, (song, userId, firstName, lastName))

    if verbose:
        print("\nQuery 3: Give me every user name (first and last) in my music app\
 history who listened to the song 'All Hands Against His Own'")
        rows = session.execute(query_3, ('All Hands Against His Own', ))
        for row in rows:
            print(f'\tuser first name: {row.firstname:>10},  user last name: {row.lastname}')                    
            
def main():
    session, cluster = connect_db()
    
    file_name = 'event_data_file_new'
    file_path = os.getcwd() + '/'+file_name+'.csv'
    
    data = DataPrep(
        filepath_in=os.getcwd() + '/event_data', 
        filepath_out=file_name
    )

    data.write_csv()
    execute_query_1(session, file_path, sessionId=338, itemInSession=4, verbose=True)
    execute_query_2(session, file_path, userId=10, sessionId=182, verbose=True)
    execute_query_3(session, file_path, song='All Hands Against His Own', verbose=True)

    session.execute("DROP TABLE IF EXISTS song_features")
    session.execute("DROP TABLE IF EXISTS artist_song_by_user")
    session.execute("DROP TABLE IF EXISTS user_name")                                                

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()