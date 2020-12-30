# For table song_features, the itemInSession was used as a partition key because the queries will 
# filter by this column. The sessionId were used as clustering column to help make up a unique key.
#
# For table artist_song_by_user, the userId and sessionId was used as a composite partition key 
# because the queries will filter by these columns. The itemInSession were used as clustering column 
# to help make up a unique key.
# 
# For table user_name, the song was used as a partition key because the queries will filter by 
# this column. The userId were used as clustering column to help make up a unique key.


# ========
#  Tables
# ========

song_features = """CREATE TABLE IF NOT EXISTS song_features (itemInSession int, 
                                sessionId int, artist text, song text,
                                length float, PRIMARY KEY(itemInSession, sessionId) )"""

artist_song_by_user = """CREATE TABLE IF NOT EXISTS artist_song_by_user (userId int, sessionId int, itemInSession int, 
                                         artist text, song text, firstName text, 
                                         lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"""

user_name = """CREATE TABLE IF NOT EXISTS user_name (song text, userId int, 
                                        firstName text, lastName text, 
                                        PRIMARY KEY (song, userId))"""

# =========
#  Inserts
# =========

insert_data_song_features = """INSERT INTO song_features (
                         itemInSession, sessionId, 
                         artist, song, length) VALUES (%s, %s, %s, %s, %s)"""

insert_data_artist_song_by_user = """INSERT INTO artist_song_by_user (
                         userId, sessionId, itemInSession, 
                         artist, song, firstName, lastName) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""

insert_data_user_name = """INSERT INTO user_name (song, userId, 
                                     firstName, lastName) VALUES (%s, %s, %s, %s)"""
    
# =========
#  Queries
# =========

query_1 = """SELECT artist, song, length 
                    FROM song_features 
                    WHERE itemInSession = %s AND sessionId = %s"""

query_2 = """SELECT artist, song, firstName, lastName 
                    FROM artist_song_by_user
                    WHERE userId = %s AND sessionId = %s"""
        
query_3 = """SELECT firstName, lastName 
                    FROM user_name 
                    WHERE song = %s"""