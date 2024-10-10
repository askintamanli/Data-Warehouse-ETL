import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES
staging_events_table_drop = "DROP TABLE staging_events;"
staging_songs_table_drop = "DROP TABLE staging_songs;"
songplay_table_drop = "DROP TABLE songplays;"
user_table_drop = "DROP TABLE users;"
song_table_drop = "DROP TABLE songs;"
artist_table_drop = "DROP TABLE artists;"
time_table_drop = "DROP TABLE time;"

# CREATE TABLES
staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR, 
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INTEGER
    );
"""

staging_songs_table_create = """ 
    CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_location VARCHAR,
    artist_longitude FLOAT,
    artist_name VARCHAR,
    duration FLOAT,
    num_songs INTEGER,
    song_id VARCHAR,
    title VARCHAR,
    year INTEGER
    );
"""

songplay_table_create = """ 
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
    );
"""

user_table_create = """ 
    CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
    );
"""

song_table_create = """ 
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT
    );
"""

artist_table_create = """ 
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
    );
"""

time_table_create = """ 
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
    );
"""

# STAGING TABLES
staging_events_copy = (
    """
    COPY staging_events 
    FROM '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON '{}'
"""
).format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH"),
)

staging_songs_copy = (
    """
    COPY staging_songs
    FROM '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON 'auto'
"""
).format(
    config.get("S3", "SONG_DATA"), 
    config.get("IAM_ROLE", "ARN")
)

# FINAL TABLES
songplay_table_insert = """
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level,
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    ) 
    SELECT 
        DISTINCT(e.ts),           
        e.userId,           
        e.level,           
        s.song_id,          
        s.artist_id,        
        e.sessionId,   
        e.location,         
        e.userAgent   
    FROM staging_events e 
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
    WHERE e.page = 'NextSong'
"""

user_table_insert = """
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    )
    SELECT
        DISTINCT(userId),    
        firstName,         
        lastName,          
        gender,
        level
    FROM staging_events
    WHERE  page = 'NextSong' and userId IS NOT NULL
"""

song_table_insert = """
    INSERT INTO songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
    SELECT
        DISTINCT(song_id),
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
"""

artist_table_insert = """
    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT  
        DISTINCT(artist_id),
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
"""

time_table_insert = """
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT
        DISTINCT(start_time)
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(dayofweek FROM start_time)
    FROM songplays
"""

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
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
