song_query_str = """ 
    SELECT DISTINCT
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
    FROM SONG_TABLE 
    WHERE song_id IS NOT NULL 
"""

artists_query_str = """ 
    SELECT DISTINCT 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
    FROM SONG_TABLE
    WHERE artist_id IS NOT NULL 
"""

users_query_str = """ 
    SELECT DISTINCT 
    userId, 
    firstName, 
    lastName, 
    gender, 
    level
    FROM LOG_TABLE 
    WHERE userId IS NOT NULL 
"""

time_query_str = """ 
    SELECT TIME.start_time_sub AS start_time, 
    hour(TIME.start_time_sub) AS hour, 
    dayofmonth(TIME.start_time_sub) AS day, 
    weekofyear(TIME.start_time_sub) AS week, 
    month(TIME.start_time_sub) AS month, 
    year(TIME.start_time_sub) AS year, 
    dayofweek(TIME.start_time_sub) AS weekday
    FROM (SELECT to_timestamp(0.001 * TIMESTAMP.ts) AS start_time_sub 
    FROM LOG_TABLE TIMESTAMP WHERE TIMESTAMP.ts IS NOT NULL) TIME 
"""

songplay_query_str = """ 
    SELECT row_number() OVER (ORDER BY "idx") AS SONGPLAY_ID, 
    to_timestamp(0.001 * LOG_TABLE.ts) AS STARTTIME, 
    month(to_timestamp(0.001 * LOG_TABLE.ts)) AS MONTH, 
    year(to_timestamp(0.001 * LOG_TABLE.ts)) AS YEAR, 
    LOG_TABLE.userId AS USERID, 
    LOG_TABLE.level AS LEVEL, 
    LOG_TABLE.sessionId AS SESSIONID, 
    LOG_TABLE.location AS LOCATION, 
    LOG_TABLE.userAgent AS USERAGENT,
    SONG_TABLE.song_id AS SONGID, 
    SONG_TABLE.artist_id AS ARTIST_ID 
    FROM LOG_TABLE
    JOIN SONG_TABLE ON LOG_TABLE.song = SONG_TABLE.TITLE AND 
    LOG_TABLE.ARTIST = SONG_TABLE.artist_name
"""
