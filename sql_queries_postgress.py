# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = """CREATE TABLE if not exists songplays(songplay_id SERIAL PRIMARY KEY,
user_id int not null,
level varchar,
song_id varchar ,
artist_id varchar,
session_id integer,
location varchar,
user_agent varchar
);"""

user_table_create = """CREATE TABLE if not exists users(user_id int PRIMARY KEY,
first_name varchar not null,
last_name varchar not null,
gender char,
level varchar
);"""

song_table_create = """CREATE TABLE if not exists songs(song_id varchar PRIMARY KEY,
title varchar not null,
artist_id varchar ,
year int,
duration float
);"""

artist_table_create = """CREATE TABLE if not exists artists(artist_id varchar PRIMARY KEY,
artist_name varchar not null,
artist_location varchar,
artist_latitude varchar,
artist_longitude varchar
);"""

time_table_create = """CREATE TABLE if not exists time(start_time timestamp PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday varchar
);"""

# INSERT RECORDS

songplay_table_insert = """insert into songplays(
user_id,level, session_id, location, user_agent, song_id, artist_id)
values(%s, %s, %s, %s, %s,%s,%s);
"""

user_table_insert = """insert into users(user_id,
first_name, last_name, gender, level)
values(%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level='free' where users.level='paid';
"""

song_table_insert = """insert into songs(song_id,
title, artist_id, duration, year)
values(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
"""

artist_table_insert = """insert into artists(
artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
values(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
"""


time_table_insert = """insert into time(
start_time,hour,day, week, month, year, weekday)
values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
"""

# FIND SONGS

song_select = ("""select s.song_id, a.artist_id from songs s left outer join artists a on s.artist_id=a.artist_id where s.title=%s and a.artist_name=%s and s.duration=%s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
