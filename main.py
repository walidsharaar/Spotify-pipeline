import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime
import datetime
import psycopg2

TOKEN = "BQBDRXvVrhgNp_T62VQIVc4NSZdvVjWjNP-DMCoGs_ddZeXcjknzr35WUKClxZJMH7ncLgMBih1gJ1XaTEIr5s8Pg_BTxdNxvVJHpL6m4UPuNG7vWtZHQ1qUyIOLVjkKieAV8-tHdlHWZco1FySgNmNM_MEe5f3LWcQf"

def check_if_valid_data(df: pd.DataFrame)->bool:
  #check if dataframe is empty
  if df.empty:
    print("No Song listened within 24 hours")
    return False

    #check the uniquess
    if pd.Series(df['played_at']).is_unique:
      pass
    else:
      raise Exception("Duplicate value found!")
    
    #check for the null values
    if df.isnull().values.any():
      raise Exception("Null valued found")
    
    #check the yestardays 
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps= df["timestamp"].tolist()
    for timestamp in timestamps:
      if datetime.datetime.strptime(timestamp,"%Y-%m-%d") != yesterday:
        raise Exception("At least one of the returned songs comes from 24 hours!")
    return True   




if __name__ == "__main__":
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

  # validate 
  if check_if_valid_data(song_df):
    print("Data valid, proceed to loading stage")