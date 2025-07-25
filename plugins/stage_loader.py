import json
import glob
import psycopg2


def load_songs_to_staging():
    connection = psycopg2.connect(
        host="postgres",
        dbname="postgres",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    song_paths = glob.glob('/usr/local/airflow/include/data/song_data/**/*.json', recursive=True)

    insert_sql = """
        DELETE FROM staging_songs;

        INSERT INTO staging_songs (
            num_songs, artist_id, artist_name, artist_latitude, artist_longitude,
            artist_location, song_id, title, duration, year
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for song_file in song_paths:
        with open(song_file, 'r') as file:
            song_data = json.load(file)

            params = (
                song_data.get('num_songs'),
                song_data.get('artist_id'),
                song_data.get('artist_name'),
                song_data.get('artist_latitude'),
                song_data.get('artist_longitude'),
                song_data.get('artist_location'),
                song_data.get('song_id'),
                song_data.get('title'),
                song_data.get('duration'),
                song_data.get('year')
            )

            cursor.execute(insert_sql, params)

    connection.commit()
    cursor.close()
    connection.close()


def load_events_to_staging():
    connection = psycopg2.connect(
        host="postgres",
        dbname="postgres",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    event_files = glob.glob('/usr/local/airflow/include/data/log_data/**/*.json', recursive=True)

    insert_sql = """
        DELETE FROM staging_events;
        
        INSERT INTO staging_events (
            artist, auth, firstname, gender, iteminsession, lastname, length,
            level, location, method, page, registration, sessionid, song,
            status, ts, useragent, userid
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for event_file in event_files:
        with open(event_file, 'r') as file:
            for record in file:
                event_data = json.loads(record)

                user_id = event_data.get('userId')
                user_id = int(user_id) if user_id and str(user_id).isdigit() else None

                params = (
                    event_data.get('artist'),
                    event_data.get('auth'),
                    event_data.get('firstName'),
                    event_data.get('gender'),
                    event_data.get('itemInSession'),
                    event_data.get('lastName'),
                    event_data.get('length'),
                    event_data.get('level'),
                    event_data.get('location'),
                    event_data.get('method'),
                    event_data.get('page'),
                    event_data.get('registration'),
                    event_data.get('sessionId'),
                    event_data.get('song'),
                    event_data.get('status'),
                    event_data.get('ts'),
                    event_data.get('userAgent'),
                    user_id
                )

                cursor.execute(insert_sql, params)

    connection.commit()
    cursor.close()
    connection.close()
