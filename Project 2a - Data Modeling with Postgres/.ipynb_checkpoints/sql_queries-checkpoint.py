# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id serial NOT NULL PRIMARY KEY
, start_time timestamp NOT NULL
, user_id int NOT NULL
, song_id text
, artist_id text
, session_id int NOT NULL
, level text NOT NULL
, location text NOT NULL
, user_agent text NOT NULL
, itemInSession int
, song text
, artist text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int NOT NULL PRIMARY KEY
, first_name text NOT NULL
, last_name text NOT NULL
, gender text NOT NULL
, level text NOT NULL
, ts timestamp NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id text NOT NULL PRIMARY KEY
, title text NOT NULL
, artist_id text NOT NULL
, year int NOT NULL
, duration float NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id text NOT NULL PRIMARY KEY
, name text NOT NULL
, location text
, latitude text
, longitude text
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp NOT NULL PRIMARY KEY
, hour int NOT NULL
, day int NOT NULL
, week int NOT NULL
, month int NOT NULL
, year int NOT NULL
, weekday int NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, itemInSession, song, artist) 
values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level, ts) 
values (%s, %s, %s, %s, %s, %s)ON CONFLICT (user_id) DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, gender = EXCLUDED.gender, level = EXCLUDED.level, ts = EXCLUDED.ts;
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) 
values(%s, %s, %s, %s, %s)ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude) 
values(%s, %s, %s, %s, %s) 
ON CONFLICT (artist_id) DO UPDATE SET name = EXCLUDED.name, location = EXCLUDED.location, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude;
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday) 
values(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
song_select = ("""
SELECT a.song_id, a.artist_id 
FROM songs a INNER JOIN artists b 
ON a.artist_id = b.artist_id 
WHERE a.title = %s AND b.name = %s AND a.duration = %s ;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]