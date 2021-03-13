import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSON_PATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
REGION = config.get("GEO", "REGION")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                CREATE TABLE staging_events (
                artist varchar,
                auth varchar, 
                firstName varchar,
                gender varchar,   
                itemInSession int,
                lastName varchar,
                length float,
                level varchar, 
                location varchar,
                method varchar,
                page varchar,
                registration varchar,
                sessionId int,
                song varchar,
                status int,
                ts timestamp,
                userAgent varchar,
                userId int);
                """)

staging_songs_table_create = ("""
                CREATE TABLE staging_songs (
                num_songs int,
                artist_id varchar,
                artist_latitude float,
                artist_longitude float,
                artist_location varchar,
                artist_name varchar,
                song_id varchar,
                title varchar,
                duration float,
                year int);
                """)

user_table_create = ("""
                     CREATE TABLE users (
                     user_id int PRIMARY KEY,
                     first_name varchar,
                     last_name varchar,
                     gender varchar,
                     level varchar);
                     """)

artist_table_create = ("""
                        CREATE TABLE artists (
                        artist_id varchar PRIMARY KEY,
                        name varchar SORTKEY,
                        location varchar,
                        latitude float,
                        longitude float);
                        """)

song_table_create = ("""
                    CREATE TABLE songs (
                    song_id varchar PRIMARY KEY,
                    title varchar SORTKEY,
                    artist_id varchar NOT NULL DISTKEY,
                    year int,
                    duration float);
                    """)

time_table_create = (""" 
                    CREATE TABLE time (
                    start_time date PRIMARY KEY SORTKEY,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday varchar);
                    """)

songplay_table_create = ("""
                CREATE TABLE songplays (
                songplay_id int SORTKEY,
                start_time date,
                user_id int NOT NULL,
                level varchar,
                song_id varchar,
                artist_id varchar,
                session_id int,
                location varchar,
                user_agent varchar);
                """)

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    region {}
    iam_role '{}'
    compupdate off statupdate off
    format as json {}
    timeformat as 'epochmillisecs'
""").format(LOG_DATA,REGION,ARN,LOG_JSON_PATH)

staging_songs_copy = ("""
    copy staging_songs 
    from {}
    region {}
    iam_role '{}'
    compupdate off statupdate off
    format as json 'auto'
""").format(SONG_DATA,REGION,ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT
            e.ts AS start_time, 
            e.userId AS user_id, 
            e.level AS level, 
            s.song_id AS song_id, 
            s.artist_id AS artist_id, 
            e.sessionId AS session_id, 
            e.location AS location, 
            e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_songs  s ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT
            artist_id AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT
            start_time AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
