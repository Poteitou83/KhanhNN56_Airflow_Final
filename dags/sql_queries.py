class SqlQueries:
    songplay_table_insert = ("""
        DELETE FROM songplays;  
                             
        INSERT INTO songplays (
            playid,
            start_time,
            userid,
            level,
            songid,
            artistid,
            sessionid,
            location,
            user_agent
        )
        SELECT DISTINCT
            md5(COALESCE(events.sessionid::text, '') || COALESCE(events.start_time::text, '')) AS playid,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page = 'NextSong' AND userid IS NOT NULL AND sessionid IS NOT NULL AND ts IS NOT NULL
        ) events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
            AND events.artist = songs.artist_name
    """)

    user_table_insert = ("""
        DELETE FROM users;                
        
        INSERT INTO users (
            userid,
            first_name,
            last_name,
            gender,
            level
        )
        SELECT DISTINCT ON (userid)
            userid,
            firstname,
            lastname,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong'
            AND userid IS NOT NULL;
    """)

    song_table_insert = ("""
        DELETE FROM songs;                     

        INSERT INTO songs (
            songid,
            title,
            artistid,
            year,
            duration
        )
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
        DELETE FROM artists;
        
        INSERT INTO artists (
            artistid,
            name,
            location,
            lattitude,
            longitude
        )
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
        DELETE FROM time;
                         
        INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT
            start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(dow FROM start_time)
        FROM songplays
        WHERE start_time IS NOT NULL;
    """)
