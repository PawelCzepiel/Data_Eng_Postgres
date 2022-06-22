import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
  Description: This function can be used to read the file in the filepath (data/songs_data)
  to get the song&artist related metadata and use it to populate the users and artists dim tables.

  Arguments:
      cur: the cursor object. 
      filepath: songs data file path. 

  Returns:
      None
  """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
  Description: This function can be used to read the file in the filepath (data/log_data)
  to get the user and time info and used to populate the users and time dim tables.

  Arguments:
      cur: the cursor object. 
      filepath: log data file path. 

  Returns:
      None
  """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']
    df['ts'] = pd.to_datetime(df['ts'], unit = 'ms')    
    # convert timestamp column to datetime
    t = df['ts']
    
    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek))
    column_labels = ['timestamp','hour','day','week','month','year','day_of_week']
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
  Description: The purpose of this function is to:
            - get the list of all json files from a given filepath
            - for each file name execute nested function process_song_file/process_log_file
                in order to populate all database dim tables as well as fact table.
  Arguments:
      cur: the cursor object. 
      conn: Postgres database connection object
      filepath: songs_data/log_data file path. 
      func: nested function: process_song_file orprocess_log_file

  Returns:
      None
  """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
  Description: The purpose of this function is to execute all
                above function in the proper order and given input:
                - establish connection to Posgres database
                - create cursor
                - run process_song_file function
                - run process_log_file function

  Arguments:
      None

  Returns:
      None
  """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()