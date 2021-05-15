import os
import glob
import psycopg2
import pandas as pd
import numpy as np
import json
import datetime
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)



def process_song_file(cur, filepath):
    """
        This function open a file located under 'data/song_data'  and add its data into two dimension tables, songs and artists
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title", "artist_id","duration","year"]].values.tolist()
    song_data=song_data[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values.tolist()
    artist_data=artist_data[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        This function open a file located under 'data/process_data'  and add its data into two dimension tables, time, users and the fact table , songplays
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t =pd.Series(pd.to_datetime(df["ts"].values, unit="ms"), index=None)
    
    # insert time data records
    time_data = [t,t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.day_name()]
    #time_data = [t,t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.day_name()]
    column_labels = ("date_time","hour", "day", "week", "month", "year", "weekday")
    # changing as suggested by the reviewer
    time_dict = {"date_time":t,"hour":t.dt.hour,"day":t.dt.day,"week":t.dt.weekofyear,"month":t.dt.month,"year":t.dt.year,"weekday":t.dt.day_name()}
    #time_dict = {"date_time":t,"hour":t.dt.hour,"day":t.dt.day,"week":t.dt.isocalendar().week,"month":t.dt.month,"year":t.dt.year,"weekday":t.dt.day_name()}
    time_df=pd.DataFrame(time_dict).drop_duplicates()
        
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # load user table
    user_df = df[["userId", "firstName", "lastName","gender","level"]].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results= cur.fetchone()
        #print(results)
        if results:
              songid, artistid = results
        else:
              songid, artistid = None, None
            
        # insert songplay record
        #df["ts"]=pd.to_datetime(df["ts"],unit="ms")
        #songplay_data = df[["userId","level","sessionId","location","userAgent"]].values.tolist()
        #songplay_data=songplay_data[0]
        #songplay_data.append(songid)
        #songplay_data.append(artistid)
        songplay_data=(row.userId,row.level, row.sessionId, row.location,row.userAgent,songid, artistid) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        This function processes all the files located under the two defined paths, 'data/song_data' and 'data/log_data'
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
       This main function calls the above 3 function to process the files into the 5 tables of Sparkify Schema
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb ")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
