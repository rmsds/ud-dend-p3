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
staging_events_table_create = ("""
    create table staging_events (
    artist_name varchar(500),
    auth varchar(30),
    firstName varchar(50),
    gender varchar(1),
    iteminSession int,
    lastName varchar(50),
    length numeric,
    level varchar(4),
    location varchar(100),
    method varchar(3),
    page varchar(40),
    registration numeric,
    sessionId int,
    song varchar(300),
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
    artist_location varchar(300),
    artist_name varchar(500),
    song_id char(18),
    title varchar(300),
    duration numeric,
    year smallint);
""")

songplay_table_create = ("""
    create table songplay (
    songplay_id int identity(0,1) primary key,
    start_time timestamp not null sortkey,
    user_id int not null,
    level varchar(4),
    song_id char(18),
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
    song_id char(18) primary key,
    title varchar(300),
    artist_id char(18),
    year smallint,
    duration numeric);
""")

artist_table_create = ("""
    create table artists (
    artist_id char(18) primary key,
    name varchar(500),
    location varchar(300),
    latitude numeric(7,4),
    longitude numeric(7,4));
""")

time_table_create = ("""
    create table time (
    start_time timestamp primary key,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    iam_role '{}'
    format as json '{}'
    timeformat as 'epochmillisecs';
    """).format(
            config['S3']['log_data'],
            config['IAM_ROLE']['arn'],
            config['S3']['log_jsonpath'],
    )

staging_songs_copy = ("""
    copy staging_songs from '{}'
    iam_role '{}'
    format as json 'auto';
""").format(
        config['S3']['song_data'],
        config['IAM_ROLE']['arn'],
    )

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplay (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    select
        se.ts,
        cast(se.userId as int),
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    from staging_events as se
    inner join staging_songs as ss
        on se.artist_name = ss.artist_name
        and se.song = ss.title
    where se.page = 'NextSong';
""")

user_table_insert = ("""
    insert into users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    select
        se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    from staging_events as se
    where se.userId is not null
        and se.page = 'NextSong';
""")

song_table_insert = ("""
    insert into songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    select
        sg.song_id,
        sg.title,
        sg.artist_id,
        sg.year,
        sg.duration
    from staging_songs as sg;
""")

artist_table_insert = ("""
    insert into artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    select
        sg.artist_id,
        sg.artist_name,
        sg.artist_location,
        sg.artist_latitude,
        sg.artist_longitude
    from staging_songs as sg;
""")

time_table_insert = ("""
    insert into time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    select
        se.ts,
        extract(hour    from se.ts),
        extract(day     from se.ts),
        extract(week    from se.ts),
        extract(month   from se.ts),
        extract(year    from se.ts),
        extract(weekday from se.ts)
    from staging_events as se
    where se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [
        staging_events_table_create,
        staging_songs_table_create,
        songplay_table_create,
        user_table_create,
        song_table_create,
        artist_table_create,
        time_table_create,
]
drop_table_queries = [
        staging_events_table_drop,
        staging_songs_table_drop,
        songplay_table_drop,
        user_table_drop,
        song_table_drop,
        artist_table_drop,
        time_table_drop,
]
copy_table_queries = [
        staging_events_copy,
        staging_songs_copy,
]
insert_table_queries = [
        songplay_table_insert,
        user_table_insert,
        song_table_insert,
        artist_table_insert,
        time_table_insert,
]
