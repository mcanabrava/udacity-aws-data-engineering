import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (
   """
   CREATE TABLE staging_events_table (
      stagingEventId INT PRIMARY KEY,
      artist VARCHAR(500),
      auth VARCHAR(20),
      firstName VARCHAR(500),
      gender CHAR(1),
      itemInSession SMALLINT,
      lastName VARCHAR(500),
      length NUMERIC,
      level VARCHAR(20),
      location VARCHAR(500),
      method VARCHAR(20),
      page VARCHAR(500),
      registration NUMERIC,
      sessionId SMALLINT,
      song VARCHAR,
      status SMALLINT,
      ts BIGINT,
      userAgent VARCHAR(500),
      userId SMALLINT
    )
   """
)

staging_songs_table_create = (
   """
   CREATE TABLE staging_songs_table (
      staging_song_id bigint PRIMARY KEY,
      num_songs INTEGER NOT NULL,
      artist_id VARCHAR(20) NOT NULL,
      artist_latitude NUMERIC,
      artist_longitude NUMERIC,
      artist_location VARCHAR(500),
      artist_name VARCHAR(500) NOT NULL,
      song_id VARCHAR(20) NOT NULL,
      title VARCHAR(500) NOT NULL,
      duration NUMERIC NOT NULL,
      year SMALLINT NOT NULL
   );
   """
)

songplay_table_create = (
   """
   CREATE TABLE songplays (
      songplay_id BIGINT PRIMARY KEY, 
      start_time BIGINT REFERENCES time(start_time) distkey, 
      user_id SMALLINT REFERENCES users(user_id), 
      level VARCHAR(20), 
      song_id VARCHAR(20) REFERENCES songs(song_id), 
      artist_id VARCHAR(20) REFERENCES artists(artist_id), 
      session_id SMALLINT, 
      location VARCHAR(500), 
      user_agent VARCHAR(500)
   )
   """
)

user_table_create = (
   """
   CREATE TABLE users (
      user_id INT PRIMARY KEY, 
      first_name VARCHAR(500),
      last_name VARCHAR(500),
      gender CHAR(1),
      level VARCHAR(20) NOT NULL
   )
   diststyle all
   sortkey(gender, first_name, last_name);
   """
)

song_table_create = (
   """
   CREATE TABLE songs (
      song_id VARCHAR(20) PRIMARY KEY, 
      title VARCHAR(500) NOT NULL,
      artist_id VARCHAR(20) NOT NULL,
      year SMALLINT NOT NULL,
      duration NUMERIC NOT NULL
   )
   sortkey(year, title);
   """
)

artist_table_create = (
   """
   CREATE TABLE artists (
      artist_id VARCHAR(20) PRIMARY KEY, 
      name VARCHAR(500) NOT NULL,
      location VARCHAR(500),
      latitude NUMERIC,
      longitude NUMERIC
   )
   diststyle all
   sortkey(name, location);
   """
)

time_table_create = (
   """
   CREATE TABLE time (
      start_time timestamp PRIMARY KEY distkey, 
      hour SMALLINT NOT NULL,
      day SMALLINT NOT NULL,
      week SMALLINT NOT NULL,
      month SMALLINT NOT NULL,
      year SMALLINT NOT NULL,
      weekday SMALLINT NOT NULL
   )
   sortkey(year, month, day);
   """
)

# STAGING TABLES

staging_events_copy = (
   """
   copy staging_events_table (
      artist, auth, firstName, gender,itemInSession, lastName, 
      length, level, location, method, page, registration, 
      sessionId, song, status, ts, userAgent, userId
   )
   from {}
   iam_role {}
   json {} region 'us-west-2';
   """
).format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = (
   """
   copy staging_songs_table 
   from {}
   iam_role {}
   json 'auto' region 'us-west-2';
   """
).format(config['S3']['song_data'], config['IAM_ROLE']['arn'])

# FINAL TABLES

songplay_table_insert ="""
   INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
   SELECT  e.ts start_time, e.userId user_id, e.level, s.song_id, s.artist_id, 
            e.sessionId session_id, e.location, e.userAgent user_agent
    FROM staging_events_table e
    JOIN staging_songs s ON (e.song = s.title)
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId user_id, firstName first_name, lastName last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name name, artist_location AS location,
            artist_latitude AS latitude, artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(DAYOFWEEK FROM start_time) AS weekday
    FROM songplays;
"""


# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create,
                       songplay_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert,
                        songplay_table_insert]
