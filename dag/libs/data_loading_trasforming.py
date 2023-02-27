import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import json
from airflow.models.connection import Connection
import psycopg2
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import sqlite3
import os.path
import os
import pandas as pd
import numpy as np
import re
from airflow.models import BaseOperator
import requests
from bs4 import BeautifulSoup as Soup
import psycopg2.extras as extras
from airflow.utils.decorators import apply_defaults
from sqlalchemy.orm import sessionmaker
from sqlalchemy import delete



"""Reading data from sqlite database and csv files we scraped and loading them into postgres database

    Args:
        postgres_user : user name for postgres login.
        postgres_password : password for the provided user name.
        postgres_host : database host.
        postgres_port : database port.
        postgres_database : database name.
"""
class DataLoadingAndTransforming(BaseOperator):
    
    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 postgres_user = "",
                 postgres_password = "",
                 postgres_host = "",
                 postgres_port = "",
                 postgres_database = "",
                 *args, **kwargs):
        super(DataLoadingAndTransforming, self).__init__(*args, **kwargs)

        # Mapping prameters
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_database = postgres_database


    def execute(self, context):

        self.log.info('Importing Scraped Teams and Players ..')

        # Reading scraped files
        df_scraped_teams = pd.read_csv('../scraped_teams.csv')
        df_scraped_players = pd.read_csv('../scraped_players.csv')

        self.log.info('Transformin Scraped Players ..')

        # Converting Date column to datetime format
        df_scraped_players['Date'] = pd.to_datetime(df_scraped_players['Date'])

        # Scraped age was at the form of ['28'] so we want to extract the number
        df_scraped_players['At_the_time_Age'] = df_scraped_players.apply(lambda row: row['At_the_time_Age'][2:4], axis = 1) 
        
        # Converting age to integer 
        df_scraped_players['At_the_time_Age'] = df_scraped_players['At_the_time_Age'].astype(int)

        # Converting rating to int
        df_scraped_players['Overall_Rating'] = df_scraped_players['Overall_Rating'].astype(int)

        # Scraped wage has the euro sign included, so we need to remove it 
        df_scraped_players['Wage'] = df_scraped_players['Wage'].map(lambda x: str(x)[1:])

        # In order to convert wage to a number we need to remove K character
        df_scraped_players['Wage'] = df_scraped_players['Wage'].map(lambda x: str(x).replace('K','000'))

        # Converting wage to integer
        df_scraped_players['Wage'] = df_scraped_players['Wage'].astype(int)

        
        self.log.info('Connecting to SQLite ..')

        # Connecting to sqlite database 
        lite_database = os.path.abspath("../../database.sqlite")
        lite_conn = sqlite3.connect(lite_database)
        lite_cur = lite_conn.cursor()

        self.log.info('Transformin Scraped Teams ..')
        df_team = pd.read_sql('Select * from Team', lite_conn)

        # Converting ID to integer
        df_scraped_teams['ID'] = df_scraped_teams['ID'].astype(float) 

        # Joining the scraped teams data and the one in the sqlite database
        df_merged_teams = pd.merge(df_scraped_teams, df_team, how='outer', left_on = 'ID', right_on = 'team_fifa_api_id')

        # Filling null team names
        df_merged_teams.Team_Name.fillna(df_merged_teams.team_long_name, inplace=True)

        # Filling null api id
        df_merged_teams.ID.fillna(df_merged_teams.team_fifa_api_id, inplace=True)

        # Drop any duplicates
        df_merged_teams.drop_duplicates(subset='ID',keep="first")


        self.log.info('Connecting to Postgres ..')

        # Connecting to postgres european_soccer database
        conn_string = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(self.postgres_user, self.postgres_password, self.postgres_host, self.postgres_port, self.postgres_database)
        engine = create_engine(conn_string, pool_size=5, pool_recycle=3600)
        postgres_conn = engine.connect()

        # Truncating table dim_team to avoid any conflicts
        self.log.info('Truncating dim_team ..')
        sql = text("DELETE FROM dim_team")
        engine.execute(sql)        
        df_merged_teams = df_merged_teams[['ID','team_api_id','Team_Name']]
        df_merged_teams.rename(columns={'ID': 'team_fifa_id', 'team_api_id': 'team_api_id', 'Team_Name': 'team_name'}, inplace=True)

        # Inserting merged data to postgres
        df_merged_teams.to_sql('dim_team', engine ,if_exists='append',index=False ,chunksize=100)


        # Truncating table dim_player
        self.log.info('Truncating dim_player ..')
        sql = text("DELETE FROM dim_player")
        engine.execute(sql) 
        df_Player = pd.read_sql("""Select * 
                                   from Player""", lite_conn)
        df_Player = df_Player[['player_fifa_api_id','player_name','birthday','height','weight']]
        df_Player.rename(columns={'player_fifa_api_id': 'player_fifa_id','birthday': 'age'}, inplace=True)
        
        self.log.info('Inserting dim_player ..')
        
        # Inserting data to postgres
        df_Player.to_sql('dim_player', engine ,if_exists='append',index=False ,chunksize=100)
       

        # Truncating table dim_player_attr
        self.log.info('Truncating dim_player_attr ..')
        sql = text("DELETE FROM dim_player_attr")
        engine.execute(sql) 
        df_scraped_players = df_scraped_players[['ID','At_the_time_Age','Overall_Rating','Team','Sub','Wage','Total_Stats','Date']]
        df_scraped_players.rename(columns={'ID': 'player_fifa_id','At_the_time_Age': 'at_the_time_age','Overall_Rating':'overall_rating','Team':'team','Sub':'contract_time','Wage':'wage','Total_Stats':'total_stats','Date':'info_date'}, inplace=True)
        
        # Inserting data to postgres
        self.log.info('Inserting dim_player_attr ..')
        df_scraped_players.to_sql('dim_player_attr', engine ,if_exists='append',index=False ,chunksize=100)
       
        # Truncating table dim_team_attr
        self.log.info('Truncating dim_team_attr ..')
        sql = text("DELETE FROM dim_team_attr")
        engine.execute(sql) 
        # Selecting only needed columns
        df_team_attr = pd.read_sql("""SELECT 
                                        team_api_id
                                        ,buildUpPlaySpeed as build_up_play_speed
                                        ,buildUpPlayDribbling as build_up_play_dribbling
                                        , buildUpPlayPassing as build_up_play_passing
                                        ,defencePressure as deffense_pressure
                                        ,defenceAggression as deffense_aggression
                                        ,defenceTeamWidth as deffense_team_width
                                        , date as info_date
                                    FROM Team_Attributes""", lite_conn)
        
        self.log.info('Inserting dim_team_attr ..')
        # Inserting data to postgres
        df_team_attr.to_sql('dim_team_attr', engine ,if_exists='append',index=False ,chunksize=100)
       

        # Truncating table dim_league
        self.log.info('Truncating dim_league ..')
        sql = text("DELETE FROM dim_league")
        engine.execute(sql) 
        # selecting columns and renaming them 
        df_league = pd.read_sql("""select 
                                    l.country_id as league_id
                                    , c.name as country_name
                                    , l.name as league_name 
                                from League l 
                                inner join country c on l.country_id = c.id""", lite_conn)
        self.log.info('Inserting dim_league ..')
        #Insering data to postgres
        df_league.to_sql('dim_league', engine ,if_exists='append',index=False ,chunksize=100)
       

        # Truncating table dim_time
        self.log.info('Truncating dim_time ..')
        sql = text("DELETE FROM dim_time")
        engine.execute(sql) 
        # Selecting match_date
        df_time = pd.read_sql('select distinct date from Match', lite_conn)
        # Converting to datetime
        df_time['date'] = pd.to_datetime(df_time.date, format='%Y-%m-%d')
        column_labels =  ['match_date','day','week','month','year','weekday']
        # Extracting day, week, ..
        df_time_updated = pd.DataFrame(columns = column_labels)
        df_time_updated['match_date'] = df_time['date']
        df_time_updated['day'] = df_time['date'].dt.day
        df_time_updated['week'] = df_time['date'].dt.weekday
        df_time_updated['month'] = df_time['date'].dt.month
        df_time_updated['year'] = df_time['date'].dt.year
        df_time_updated['weekday'] = df_time['date'].dt.day_name()
        # Inserting data to postgres
        self.log.info('Inserting dim_time ..')
        df_time_updated.to_sql('dim_time', engine ,if_exists='append',index=False ,chunksize=100)
       
        # Truncating table fct_match
        self.log.info('Truncating fct_match ..')
        sql = text("DELETE FROM fct_match")
        engine.execute(sql) 
        # Selecting needed columns
        df_match = pd.read_sql("""SELECT
                                    match_api_id as match_id
                                    ,league_id 
                                    ,season 
                                    ,stage 
                                    ,date as match_date
                                    ,home_team_api_id 
                                    ,away_team_api_id
                                    ,home_team_goal
                                    ,away_team_goal 
                                from Match""", lite_conn)

        self.log.info('Inserting fct_match ..')
        # Inserting data to postgres
        df_match.to_sql('fct_match', engine ,if_exists='append',index=False ,chunksize=100)
       


        self.log.info('Data Inserted Successfully ..')

        
        

        



