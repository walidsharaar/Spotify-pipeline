import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime
import datetime
import psycopg2

TOKEN = "BQCyXZplExde2xfi1ocBQJw77I-EKk9A_h7wMydDoAjn4EwXsxLnwmKVUWjr3iz1bYzDAKSqI66qH6zOqmDZN0s7rlOkdZJsWaDB7kJuDV-cFSJweVPVa6THV0iCaRyHnmL8Hodfbeeo7HkavpOir7LW-l_8qID_OCFw"

  # Extract part of the ETL process
 
headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",
    "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

# Convert time to Unix timestamp in miliseconds      
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

  # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
request = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp),
headers = headers)
data = request.json()
print(data)

# field to store in database
song_names=[]
artist_names=[]
played_at_list=[]
timestamps=[]

# looping the fetch data to get the required data

for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

       
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
song_dict = {
    "song_name" : song_names,
    "artist_name": artist_names,
    "played_at" : played_at_list,
    "timestamp" : timestamps
    }

#create datafram using pandas

song_df = pd.DataFrame(song_dict, columns=["song_name","artist_name","played_at","timestamp"])
print(song_df)