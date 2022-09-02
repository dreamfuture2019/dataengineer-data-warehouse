import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#Create Table: staging_events
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist          TEXT, 
    auth            TEXT,
    firstName       TEXT,
    gender          VARCHAR(2), 
    itemInSession   INTEGER,
    lastName        TEXT,
    length          NUMERIC,
    level           VARCHAR(5),
    location        TEXT, 
    method          VARCHAR(5), 
    page            TEXT, 
    registration    NUMERIC,
    sessionId       INTEGER NOT NULL SORTKEY DISTKEY,
    song            TEXT,
    status          INTEGER,
    ts              BIGINT,
    userAgent       TEXT,
    userId          BIGINT
)
""")
#Create Table: staging_songs
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(    
    artist_id         VARCHAR(25) NOT NULL SORTKEY DISTKEY,
    artist_latitude   DOUBLE PRECISION, 
    artist_longitude  DOUBLE PRECISION,
    artist_location   TEXT, 
    artist_name       TEXT,
    song_id           VARCHAR(25),
    title             TEXT, 
    page              TEXT,
    duration          NUMERIC,
    year              SMALLINT,
    num_songs         SMALLINT
)
""")

#Create Table: songplays
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id   BIGINT IDENTITY(1,1) PRIMARY KEY, 
    start_time    TIMESTAMP NOT NULL SORTKEY, 
    user_id       INTEGER NOT NULL DISTKEY, 
    level         VARCHAR(7), 
    song_id       VARCHAR(25), 
    artist_id     VARCHAR(25), 
    session_id    INTEGER, 
    location      TEXT, 
    user_agent    TEXT
) diststyle key
""")

#Create Table: users
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id        BIGINT PRIMARY KEY SORTKEY, 
    first_name     TEXT, 
    last_name      TEXT, 
    gender         VARCHAR(7), 
    level          VARCHAR(7)
) diststyle all
""")

#Create Table: songs
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id       VARCHAR(25) PRIMARY KEY SORTKEY, 
    title         TEXT NOT NULL, 
    artist_id     VARCHAR(25) DISTKEY, 
    year          SMALLINT, 
    duration      NUMERIC NOT NULL
) diststyle key
""")

#Create Table: artists
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id     VARCHAR(25) PRIMARY KEY SORTKEY, 
    name          TEXT NOT NULL, 
    location      TEXT, 
    latitude      DOUBLE PRECISION, 
    longitude     DOUBLE PRECISION
) diststyle all
""")

#Create Table: time
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time    TIMESTAMP PRIMARY KEY SORTKEY, 
    hour          SMALLINT, 
    day           SMALLINT, 
    week          SMALLINT, 
    month         SMALLINT, 
    year          SMALLINT DISTKEY, 
    weekday       SMALLINT
) diststyle key
""")

# STAGING TABLES
# Load events data from s3 into staging table in Redshift
staging_events_copy = ("""
copy {} from {} 
credentials {}
JSON {} region 'us-west-2';
""").format('staging_events', LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

# Load songs data from s3 into staging table in Redshift
staging_songs_copy = ("""
copy {} from {} 
credentials {}
JSON 'auto' region 'us-west-2';
""").format('staging_songs', SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays 
    (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    )
    SELECT  
            TIMESTAMP 'epoch' + event.ts/1000 * interval '1 second' as start_time, 
            event.userId, 
            event.level, 
            song.song_id,
            song.artist_id, 
            event.sessionId,
            event.location, 
            event.userAgent
    FROM staging_events event, staging_songs song
    WHERE event.page = 'NextSong' 
    AND event.song = song.title 
    AND event.artist = song.artist_name 
    AND event.length = song.duration
""")

user_table_insert = ("""
    INSERT INTO users
    (
        user_id, first_name, last_name, gender, level
    ) 
    SELECT DISTINCT  
            userId, 
            firstName, 
            lastName, 
            gender, 
            level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs
    (
        song_id, title, artist_id, year, duration
    )
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists
    (
        artist_id, name, location, latitude, longitude
    )
    SELECT DISTINCT 
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time
    (
        start_time, hour, day, week, month, year, weekday
    )
    SELECT start_time, 
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time), 
            extract(month from start_time),
            extract(year from start_time), 
            extract(dayofweek from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
