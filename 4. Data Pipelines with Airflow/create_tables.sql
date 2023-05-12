staging_events_table_create= (
   """
   CREATE TABLE public.staging_events_table (
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
   CREATE TABLE public.staging_songs_table (
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


user_table_create = (
   """
   CREATE TABLE public.users (
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
   CREATE TABLE public.songs (
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
   CREATE TABLE public.artists (
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
   CREATE TABLE public.time (
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

songplay_table_create = (
   """
   CREATE TABLE public.songplays (
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