# Spotify Pipline
In this project I will implement the entire process of the ETL (Extract, Transform and Load) process based on Spotify API data. Also, I am going to implement basic Data Engineering concepts within project.

I am going to build a simple data pipeline (or in other words, a data feed) that downloads Spotify data on what songs I've listened to in the last 24 hours, and saves that data in a SQLite database.

I will schedule this pipeline to run daily. After a few months I will end up with our own, private Spotify played tracks history dataset!

## 1. Get Data
I am using spotify api for getting the data from the spotify.
Documentation is as below: 
https://developer.spotify.com/console/get-recently-played/

## 2. Automation
Airflow quick start guide:
https://airflow.apache.org/docs/apache-airflow/stable/start/index.html

