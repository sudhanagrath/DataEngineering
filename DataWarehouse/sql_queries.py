import configparser


# CONFIG
#Reading the configuration parameters
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
#query strings to define the drop table for each of the 7 tables
staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES
#query strings to define the create table for each of the 7 tables
staging_events_table_create= """CREATE TABLE if not exists staging_events(event_id bigint IDENTITY(0,1) PRIMARY KEY,
artist text,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length float,
level text,
location text,
method text,
page text,
registration bigint,
sessionId int not null,
song text,
status int,
ts bigint not null,
userAgent text,
userId int);"""

staging_songs_table_create = """CREATE TABLE if not exists staging_songs(num_songs int,
artist_id text,
artist_latitude numeric,
artist_longitude numeric,
artist_location text,
artist_name text,
song_id text,
title text,
duration float,
year int) diststyle even;"""

user_table_create = """CREATE TABLE if not exists users(userId int PRIMARY KEY,
firstName text,gender text,lastName text,level text );"""

song_table_create = """CREATE TABLE if not exists songs(song_id text PRIMARY KEY,
title text not null,
artist_id text ,
year int,
duration float
);"""

artist_table_create = """CREATE TABLE if not exists artists(artist_id text PRIMARY KEY,
artist_name text not null,
artist_location text,
artist_latitude numeric,
artist_longitude numeric
);"""

time_table_create="""CREATE TABLE if not exists time(start_time timestamp PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday text
);"""

songplay_table_create = """CREATE TABLE if not exists songplays(songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
start_time timestamp not null,
user_id int not null,
level text,
song_id text not null,
artist_id text not null,
session_id integer not null,
location text,
user_agent text,
foreign key (start_time) references time(start_time),
foreign key (user_id) references users(userId),
foreign key (artist_id) references artists(artist_id),
foreign key (song_id) references songs(song_id)
);"""


# STAGING TABLES
#Copy Load the log-data objects from S3 Buckets to Staging Events table
staging_events_copy = """copy staging_events from {} 
credentials 'aws_access_key_id={};aws_secret_access_key={}'  region 'us-west-2' json {};
""".format(config['S3_LOG_DATA']['LOG_DATA'], config['AWS']['KEY'],  config['AWS']['SECRET'], config['S3_LOG_DATA']['LOG_JSONPATH'])

#Copy Load the song-data objects from S3 Buckets to Staging Songs table
#The for loop copies each of the 26 keys listed in dwh.cfg to datawarehouse in order to handle massive dataset.
copy_table_queries=[]
for key in config['S3_SONG_DATA']:
	staging_songs_copy="""copy staging_songs from {}
credentials 'aws_access_key_id={};aws_secret_access_key={}' region 'us-west-2' json 'auto' truncatecolumns compupdate off;
""".format(config['S3_SONG_DATA'][key],config['AWS']['KEY'],config['AWS']['SECRET'] )
	copy_table_queries.append(staging_songs_copy)


# FINAL TABLES
#Bulk Load the users table from staging events table
user_table_insert = """insert into users(userId, firstName,gender, lastName, level)
select distinct userId, firstName, gender, lastName, level from staging_events where userId is not null ;
"""
#Bulk Load the songs table from staging songs table
song_table_insert = """insert into songs(song_id,
title, artist_id, duration, year)
select distinct song_id, title, artist_id, duration, year from staging_songs;
"""
#Bulk Load the artists table from staging songs table
artist_table_insert = """insert into artists(
artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs;
"""
#Transform into human readable format and Bulk Load the time data from staging events to time table
time_table_insert =""" insert into time(start_time, hour, day, week, month, year, weekday)
select distinct sn.start_time,
extract(HOUR from sn.start_time) as hour,
extract(DAY from sn.start_time) as day,
extract(WEEK from sn.start_time) as week,
extract(MONTH from sn.start_time) as month,
extract(YEAR from sn.start_time) as year,
extract(DOW from sn.start_time) as weekday
from (
select '1970-01-01'::date+ts/1000 * interval '1 second' as start_time from staging_events)sn;
"""
#Bulk load the fact table songplay from the staging tables
songplay_table_insert ="""insert into songplays(start_time,user_id, level, song_id, artist_id, session_id, location, user_agent) 
select ('1970-01-01'::date+se.ts/1000 * interval '1 second') as start_time, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
from staging_events se inner join staging_songs ss on trim(upper(se.artist))=trim(upper(ss.artist_name)) and trim(upper(se.song))=trim(upper(ss.title))
and se.length=ss.duration;
"""

# QUERY LISTS
#Lists to use in automating the DDL and ETL processes
create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, time_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, staging_events_table_drop, staging_songs_table_drop, user_table_drop, time_table_create, song_table_drop, artist_table_drop]
copy_table_queries.append(staging_events_copy)
insert_table_queries = [user_table_insert, time_table_insert, song_table_insert, artist_table_insert, songplay_table_insert]
