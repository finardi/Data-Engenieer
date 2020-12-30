import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Process song_data and artist_data DataFrames and insert
    into the tables
    
    Returns:
        none
    Arguments:
        cur: postgres connection cursor
        filepath: json path that will be read with pandas DataFrame
    """

    df = pd.read_json(filepath, lines=True)
    
    song_data = df[['song_id', 'title', 'artist_id', \
                    'year', 'duration']].values

    # song_data is a array with shape (1, number_of_cols)
    #  we flatten the array and insert the song records
    song_data = list(song_data.flatten())
    
    cur.execute(song_table_insert, song_data)
    
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                      'artist_latitude', 'artist_longitude']].values 

    # artist_data is a array with shape (1, number_of_cols)
    #  we flatten the array and insert the artist data
    artist_data = list(artist_data.flatten())
    
    cur.execute(artist_table_insert, artist_data)

    
def process_log_file(cur, filepath):
    """
    Process and insert data into time_table, user_table and song_plays
        
    Returns:
        none
    Arguments:
        cur: postgres connection cursor
        filepath: json path that will be read with pandas DataFrame
    """
    
    df = pd.read_json(filepath, lines=True)
    df = df[df.page == 'NextSong']

    # time_data stores datetime values: hour, day, 
    # week of year, month, year, and weekday 
    time_data = []

    # data_transform is a alias to df['ts']
    data_transform = pd.to_datetime(df['ts'], unit='ms') 

    # For each element in data_transform
    for data in zip(data_transform, 
                    data_transform.dt.hour, 
                    data_transform.dt.day, 
                    data_transform.dt.week,
                    data_transform.dt.month, 
                    data_transform.dt.year, 
                    data_transform.dt.weekday):
        # Insert datetime value into time_data
        time_data.append(data)
    
    column_labels = ['timestamp', 'hour', 'day', 'week', \
                     'month', 'year','weekday']

    # Time_df is a DataFrame with datetime values and columns_labels
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid,  
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Process process_song_file and process_log_file functions
        
    Returns:
        none
    Arguments:
        cur: postgres connection cursor
        conn: postgres connection
        filepath: json path that will be read with pandas DataFrame
        func: function to process data, the functions process_song_file and process_log_file
    """
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()