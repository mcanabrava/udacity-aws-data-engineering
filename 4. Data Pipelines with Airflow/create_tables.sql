CREATE TABLE public.staging_events (
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
);



CREATE TABLE public.staging_songs (
num_songs INTEGER,
artist_id VARCHAR(20),
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR(500),
artist_name VARCHAR(500),
song_id VARCHAR(20),
title VARCHAR(500),
duration NUMERIC,
year SMALLINT 
);




CREATE TABLE public.users (
user_id INT PRIMARY KEY, 
first_name VARCHAR(500),
last_name VARCHAR(500),
gender CHAR(1),
level VARCHAR(20) 
)
diststyle all
sortkey(gender, first_name, last_name);



CREATE TABLE public.songs (
song_id VARCHAR(20) PRIMARY KEY, 
title VARCHAR(500),
artist_id VARCHAR(20),
year SMALLINT,
duration NUMERIC 
)
sortkey(year, title);



CREATE TABLE public.artists (
artist_id VARCHAR(20) PRIMARY KEY, 
name VARCHAR(500),
location VARCHAR(500),
latitude NUMERIC,
longitude NUMERIC
)
diststyle all
sortkey(name, location);



CREATE TABLE public.time (
start_time timestamp PRIMARY KEY distkey, 
hour SMALLINT,
day SMALLINT,
week SMALLINT,
month SMALLINT,
year SMALLINT,
weekday SMALLINT 
)
sortkey(year, month, day);


CREATE TABLE public.songplays (
songplay_id VARCHAR(500) PRIMARY KEY, 
start_time timestamp REFERENCES time(start_time) distkey, 
user_id SMALLINT REFERENCES users(user_id), 
level VARCHAR(20), 
song_id VARCHAR(20) REFERENCES songs(song_id), 
artist_id VARCHAR(20) REFERENCES artists(artist_id), 
session_id SMALLINT, 
location VARCHAR(500), 
user_agent VARCHAR(500)
);
