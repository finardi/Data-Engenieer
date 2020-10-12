import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    # Load the song files in DataFrame
    df = pd.read_json(filepath, lines=True)
    
    # Get values of the select columns
    song_data = df[['song_id', 'title', 'artist_id', \
                    'year', 'duration']].values

    # song_data is a array with shape (1, number_of_cols)
    #  we flatten the array and insert the song records
    song_data = list(song_data.flatten())
    
    # Execute the insert data
    cur.execute(song_table_insert, song_data)
    
    # Get values of the select columns
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                      'artist_latitude', 'artist_longitude']].values 

    # artist_data is a array with shape (1, number_of_cols)
    #  we flatten the array and insert the artist data
    artist_data = list(artist_data.flatten())
    
    # Execute the insert data
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    # Load the song files in DataFrame
    df = pd.read_json(filepath, lines=True)
    
    # DataFrame filtered by NextSong values
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

    # Colums_labels is the labels to datetime values
    column_labels = ['timestamp', 'hour', 'day', 'week', \
                     'month', 'year','weekday']

    # Time_df is a DataFrame with datetime values and columns_labels
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load user dataframe
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # Insert songplay records
    for index, row in df.iterrows():

        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid,  
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    # Get all files matching extension from directory
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
