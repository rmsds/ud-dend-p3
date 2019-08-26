import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
    create table staging_events (
    artist_name varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    gender varchar(1),
    length numeric,
    iteminSession smallint,
    level varchar(4),
    location varchar(100),
    method varchar(3),
    page varchar(40),
    registration numeric,
    sessionId int,
    song varchar(50),
    status smallint,
    ts timestamp,
    userAgent varchar(200),
    userId int);
""")

staging_songs_table_create = ("""
    create table staging_songs (
    artist_id char(18),
    artist_latitude numeric(7,4),
    artist_longitude numeric(7,4),
    artist_location varchar(100),
    artist_name varchar(50),
    song_id char(18),
    title varchar(50),
    duration numeric,
    year smallint);
""")

songplay_table_create = ("""
    create table songplay (
    songplay_id int identity(0,1) primary key, 
    start_time bigint sortkey,
    user_id int,
    level varchar(4),
    song_ig int,
    artist_id char(18),
    session_id int,
    location varchar(100),
    user_agent varchar(200));
""")

user_table_create = ("""
    create table users (
    user_id int primary key,
    first_name varchar(50),
    last_name varchar(50),
    gender varchar(1),
    level varchar(4) sortkey);
""")

song_table_create = ("""
    create table songs (
    song_id int primary key,
    title varchar(50),
    artist_id int,
    year smallint,
    duration numeric);
""")

artist_table_create = ("""
    create table artists (
    artist_id char(18) primary key,
    name varchar(50),
    location varchar(100),
    latitude numeric(7,4),
    longitude numeric(7,4));
""")

time_table_create = ("""
    create table time (
    start_time bigint primary key,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint);
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
