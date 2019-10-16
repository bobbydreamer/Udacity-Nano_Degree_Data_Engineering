import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#print(config.sections())
#print(config.get('LOCAL', 'LOG_LOCAL_DATA'))

# Reading config & initializing variables
details_dict = {}
new_dict = {}
for section in config.sections():
    dic = dict(config.items(section))
    if len(dic) != 0 :
        #print( '{} - {} : {}'.format(len(dic), section, dic) )
        details_dict.update(dic)

        for k, v in details_dict.items():
            k = k.upper()
            #print('{} : {}'.format(k,v))
            new_dict[k] = v

            #print(new_dict)

            for k, v in new_dict.items():
                globals()[k] = v

                #print(' LOG_LOCAL_DATA = {}'.format(LOG_LOCAL_DATA))
    
    
# CREATE SCHEMA 
create_schema = {
    "query": "create schema if not exists dwh;",
    "message": "CREATE schema"
}

# DROP SCHEMA 
drop_schema = {
    "query": "drop schema if exists dwh;",
    "message": "DROP schema"    
}

# SET SCHEMA PATH
set_schema = {
    "query": "SET search_path = dwh;",
    "message": "Setting the schema path"
}

# DROP TABLES
staging_events_table_drop = {
    "query": "DROP TABLE IF EXISTS staging_events;",
    "message": "DROP staging_events table"
}

staging_songs_table_drop = {
    "query": "DROP TABLE IF EXISTS staging_songs;",
    "message": "DROP staging_songs table"
}

songplay_table_drop = {
    "query": "DROP TABLE IF EXISTS fact_songplay;",
    "message": "DROP fact_songplay table"
}

user_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_users;",
    "message": "DROP dim_users table"
}

song_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_songs;",
    "message": "DROP dim_songs table"
}

artist_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_artists;",
    "message": "DROP dim_artists table"
}

time_table_drop = {
    "query": "DROP TABLE IF EXISTS dim_time;",
    "message": "DROP time table"
}

# CREATE TABLES
staging_events_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS staging_events (
            event_id        INT IDENTITY(0, 1) NOT NULL,
            artist          VARCHAR,
            auth            VARCHAR,
            firstname      VARCHAR,
            gender          VARCHAR,
            itemInSession   INTEGER,
            lastname       VARCHAR,
            length          FLOAT,
            level           VARCHAR,
            location        VARCHAR,
            method          VARCHAR(4),
            page            VARCHAR DISTKEY,
            registration    BIGINT,
            sessionId       INTEGER,
            song            VARCHAR,
            status          INTEGER,
            ts              TIMESTAMP SORTKEY,
            userAgent       VARCHAR,
            userId          INTEGER
        ) ;
        """,
    "message": "CREATE staging_events"
}

staging_songs_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id       VARCHAR    NOT NULL SORTKEY,
            latitude        DECIMAL(10,6),
            location        VARCHAR,
            longitude       DECIMAL(10,6),
            artist_name     VARCHAR,
            duration        DECIMAL,
            num_songs       INTEGER,
            song_id         VARCHAR,
            title           VARCHAR,            
            year            INTEGER,
            process         VARCHAR,
            corrected_location VARCHAR,
            county          VARCHAR, 
            city            VARCHAR,
            state           VARCHAR,
            country         VARCHAR,
            country_code    VARCHAR
        )
        DISTSTYLE even;
        """,
    "message": "CREATE staging_songs"
}

songplay_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS fact_songplay (
            songplay_id     INTEGER     IDENTITY(0,1) PRIMARY KEY,
            start_time      TIMESTAMP   NOT NULL,
            user_id         INTEGER     NOT NULL,
            level           VARCHAR     NOT NULL,
            song_id         VARCHAR     NOT NULL,
            artist_id       VARCHAR     NOT NULL SORTKEY DISTKEY,
            session_id      INTEGER     NOT NULL,
            location        VARCHAR     NOT NULL,
            user_agent      VARCHAR     NOT NULL
        );""",
    "message": "CREATE fact_songplay"
}

user_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_id         INTEGER     NOT NULL PRIMARY KEY DISTKEY,
            first_name      VARCHAR     NOT NULL,
            last_name       VARCHAR     NOT NULL,
            gender          CHAR(1)     NOT NULL,
            level           VARCHAR     NOT NULL
        );""",
    "message": "CREATE dim_users"
}

song_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_songs (
            song_id         VARCHAR     NOT NULL PRIMARY KEY,
            title           VARCHAR     NOT NULL,
            artist_id       VARCHAR     NOT NULL SORTKEY DISTKEY,
            year            INTEGER     NOT NULL,
            duration        DECIMAL     NOT NULL
        );""",
    "message": "CREATE dim_songs"
}

artist_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_artists (
            artist_id       VARCHAR     NOT NULL PRIMARY KEY SORTKEY DISTKEY,
            name            VARCHAR     NOT NULL,
            location        VARCHAR,
            latitude        DECIMAL(10,6),
            longitude       DECIMAL(10,6),
            corrected_location VARCHAR,
            county          VARCHAR, 
            city            VARCHAR,
            state           VARCHAR,
            country         VARCHAR,
            country_code    VARCHAR            
        );""",
    "message": "CREATE dim_artists"
}

time_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS dim_time (
            start_time      TIMESTAMP       NOT NULL PRIMARY KEY SORTKEY DISTKEY,
            hour            INTEGER         NOT NULL,
            day             INTEGER         NOT NULL,
            week            INTEGER         NOT NULL,
            month           INTEGER         NOT NULL,
            year            INTEGER         NOT NULL,
            weekday         INTEGER         NOT NULL
        );""",
    "message": "CREATE dim_time"
}

# STAGING TABLES
# Commenting below COPY commands as encountered errors due to authentication using another alternative method. 
# Keeping this code as it could be useful ONE DAY
'''
staging_events_copy_query = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON {}
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(CLEAN_LOG_DATA, DWH_IAM_ROLE_NAME, LOG_JSONPATH)

staging_events_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_events"
}

staging_songs_copy_query = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON 'auto'
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(CLEAN_SONG_DATA, DWH_IAM_ROLE_NAME)

staging_songs_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_songs"
}
'''
staging_events_copy_query = ("""
    COPY staging_events FROM {}
    CREDENTIALS '{}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    CSV 
    delimiter ',' 
    IGNOREHEADER 1
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(CLEAN_LOG_DATA, KEYSECRET)

staging_events_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_events"
}

staging_songs_copy_query = ("""
    COPY staging_songs FROM {}
    CREDENTIALS '{}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    CSV 
    delimiter ',' 
    IGNOREHEADER 1
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(CLEAN_SONG_DATA, KEYSECRET)

staging_songs_copy = {
    "query": staging_songs_copy_query,
    "message": "COPY staging_songs"
}

# FINAL TABLES
songplay_table_insert = {
    "query": ("""
        INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT se.ts,
                        se.userId AS user_id,
                        se.level AS level,
                        ss.song_id AS song_id,
                        ss.artist_id AS artist_id,
                        se.sessionId AS session_id,
                        se.location AS location,
                        se.userAgent AS user_agent
        FROM dwh.staging_events se
        JOIN dwh.staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.userId !=0 and se.page = 'NextSong';
        """),
    "message": "INSERT fact_songplay"
}

user_table_insert = {
    "query": ("""
        INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender AS gender,
                        level AS level
        FROM staging_events
        WHERE userId !=0 and page = 'NextSong';
    """),
    "message": "INSERT dim_users"
}

song_table_insert = {
    "query": ("""
        INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id AS song_id,
                        title AS title,
                        artist_id AS artist_id,
                        year AS year,
                        duration AS duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """),
    "message": "INSERT dim_songs"
}

artist_table_insert = {
    "query": ("""
        INSERT INTO dim_artists(artist_id, name, location, latitude, longitude, corrected_location, county, city, state, country, country_code)
        SELECT DISTINCT artist_id AS artist_id,
                        artist_name AS name,
                        location AS location,
                        latitude AS latitude,
                        longitude AS longitude,
                        corrected_location,
                        county, 
                        city,
                        state,
                        country,
                        country_code                        
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """),
    "message": "INSERT dim_artists"
}

time_table_insert = {
    "query": ("""
        INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT ts,
                        EXTRACT(hour FROM ts),
                        EXTRACT(day FROM ts),
                        EXTRACT(week FROM ts),
                        EXTRACT(month FROM ts),
                        EXTRACT(year FROM ts),
                        EXTRACT(weekday FROM ts)
        FROM staging_events
        WHERE ts IS NOT NULL and userId !=0 and page = 'NextSong';
    """),
    "message": "INSERT dim_time"
}

# count rows 
count_staging_events = {
    "query" : ("""
    SELECT COUNT(*) FROM staging_events
    """),
    "message" : "Rows in Staging Events"
}


count_staging_songs = {
    "query" : ("""
    SELECT COUNT(*) FROM staging_songs
    """),
    "message" : "Rows in Staging Songs"
}


count_songplays = {
    "query" : ("""
    SELECT COUNT(*) FROM fact_songplay
    """),
    "message" : "Rows in fact_SongPlay"
}


count_users = {
    "query" : ("""
    SELECT COUNT(*) FROM dim_users
    """),
    "message" : "Rows in dim_Users"
}


count_songs = {
    "query" : ("""
    SELECT COUNT(*) FROM dim_songs
    """),
    "message" : "Rows in dim_Songs"
}


count_artists = {
    "query" : ("""
    SELECT COUNT(*) FROM dim_artists
    """),
    "message" : "Rows in dim_Artists"
}


count_time = {
    "query" : ("""
    SELECT COUNT(*) FROM dim_time
    """),
    "message" : "Rows in dim_Time"
}


count_all = {
    "query" : ("""
        select 'fact_songplay', count(*) from fact_songplay
        union all
        select 'dim_users', count(*) from dim_users
        union all
        select 'dim_songs', count(*) from dim_songs
        union all
        select 'dim_artists', count(*) from dim_artists
        union all
        select 'dim_time', count(*) from dim_time
    """) ,
    "message" : "Rows in Union All"
}

# QUERY LISTS
schema_queries = [create_schema, drop_schema, set_schema]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_rows_queries = [count_staging_events, count_staging_songs, count_songplays, count_users, count_songs, count_artists, count_time, count_all]