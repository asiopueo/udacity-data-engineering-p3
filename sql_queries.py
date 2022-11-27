import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays_fact"
user_table_drop = "DROP TABLE IF EXISTS users_dim"
song_table_drop = "DROP TABLE IF EXISTS songs_dim"
artist_table_drop = "DROP TABLE IF EXISTS artists_dim"
time_table_drop = "DROP TABLE IF EXISTS timestamps_dim"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR, 
    gender          VARCHAR, 
    itemInSession   INT, 
    lastName        VARCHAR, 
    length          VARCHAR, 
    level           VARCHAR, 
    location        VARCHAR, 
    method          VARCHAR, 
    page            VARCHAR, 
    registration    VARCHAR, 
    sessionId       INT, 
    song            VARCHAR, 
    status          INT, 
    ts              BIGINT, 
    userAgent       VARCHAR, 
    userId          INT)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs        INT, 
    artist_id        VARCHAR, 
    artist_latitude  DOUBLE PRECISION, 
    artist_longitude DOUBLE PRECISION, 
    artist_location  VARCHAR, 
    artist_name      VARCHAR, 
    song_id          VARCHAR, 
    title            VARCHAR, 
    duration         DOUBLE PRECISION, 
    year             INT)
""")

songplay_table_create = ("""
CREATE TABLE songplays_fact (
    songplay_id      INT IDENTITY(0,1) PRIMARY KEY,
    ts               BIGINT NOT NULL, 
    user_id          INT NOT NULL,
    level            VARCHAR,
    song_id          VARCHAR,
    artist_id        VARCHAR,
    session_id       INT,
    location         VARCHAR,
    user_agent       VARCHAR)
""")

user_table_create = ("""
CREATE TABLE users_dim (
    user_id          INT PRIMARY KEY,
    first_name       VARCHAR,
    last_name        VARCHAR, 
    gender           CHAR,
    level            VARCHAR)
""")

song_table_create = ("""
CREATE TABLE songs_dim (
    song_id          VARCHAR PRIMARY KEY NOT NULL,
    title            VARCHAR NOT NULL,
    artist_id        VARCHAR,
    year             INT,
    duration         DOUBLE PRECISION NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE artists_dim (
    artist_id   VARCHAR PRIMARY KEY NOT NULL,
    name        VARCHAR NOT NULL,
    location    VARCHAR,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION)
""")

time_table_create = ("""
CREATE TABLE timestamps_dim (
    ts         BIGINT PRIMARY KEY NOT NULL,
    start_time TIMESTAMP,
    hour       INT,
    day        INT,
    week       INT,
    month      INT,
    year       INT,
    weekday    INT)
""")


# STAGING TABLES
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
json 's3://udacity-dend/log_json_path.json'
region 'us-west-2'
""").format(LOG_DATA, DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
json 'auto'
region 'us-west-2'
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users_dim (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs_dim (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists_dim (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO timestamps_dim (
    ts,
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday) 
SELECT DISTINCT ts, start_time, EXTRACT(HOUR FROM start_time), EXTRACT(DAY FROM start_time), EXTRACT(WEEK FROM start_time), EXTRACT(MONTH FROM start_time), EXTRACT(YEAR FROM start_time), EXTRACT(DOW FROM start_time)
FROM (SELECT ts, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time FROM staging_events)
""")


songplay_table_insert = ("""
INSERT INTO songplays_fact (ts, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT logs.ts, logs.userId, logs.level, songs.song_id, songs.artist_id, logs.sessionId, logs.location, logs.userAgent
FROM staging_events AS logs JOIN staging_songs AS songs ON logs.artist=songs.artist_name OR logs.song=songs.title
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

