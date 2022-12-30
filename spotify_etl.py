import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import datetime
import sqlite3


def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


def refresh(self):
    query = "https://accounts.spotify.com/api/token"
    response = requests.post(query, data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                             headers={"Authorization": "Basic " + base_64})
    response_json = response.json()
    print(response_json)

    return response_json["access_token"]


def run_spotify_etl():
    database = "sqlite:///YOUR_NAME.sqlite"
    token = 'YOUR_TOKEN'

    # Extract

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_to_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}'.format(
        time=yesterday_to_timestamp), headers=headers)
    data = r.json()
    # print(data)

    song_names = []
    artist_names = []
    played_at = []
    timestamp_ = []

    for i in data["items"]:
        song_names.append(i["track"]["name"])
        played_at.append(i["played_at"])
        timestamp_.append(i["played_at"][0:10])

    for j in data["items"]:
        s = ""
        for k in range(0, len(j["track"]["album"]["artists"])):
            s = s + ", " + j["track"]["album"]["artists"][k]["name"]
        s = s.split(", ")[1:]
        s = str(s).replace("'", "").replace('"', '').strip('"')
        s = s[1:-1]
        artist_names.append(s)

    dict_song = {
        "song_name": song_names,
        "artists": artist_names,
        "played_at": played_at,
        "timestamp": timestamp_
    }

    df = pd.DataFrame(dict_song)

    # Validate
    if check_if_valid_data(df):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(database)
    conn = sqlite3.connect("my_recently_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artists VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")
