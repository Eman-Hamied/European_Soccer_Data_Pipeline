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
from airflow.utils.decorators import apply_defaults

"""Data scraping for teams and players from 2008 to 2016. Sofifa website. This function will generate to csv files 

    Args:
        None.
"""

class DataScraping(BaseOperator):
    
    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(DataScraping, self).__init__(*args, **kwargs)


    def execute(self, context):
        # Defining columns names
        column = ['ID','Name','At_the_time_Age','Overall_Rating','Team','Sub','Wage','Total_Stats','Date']
        df__scraped_players = pd.DataFrame(columns = column)

        # Looping over years from 2008 to 2016
        for offset in range(80000,170000,10000):
            self.log.info('Starting Players Scraping ..')
            for i in range(0,3):
                offset = offset + i
                # Looping over pages
                for j in range (0,30):
                    if offset in [80000,90000]:
                        # Checking on path
                        url = (f"https://sofifa.com/players?type=all&ct%5B0%5D=2&r=0{offset}&set=true")
                    else:
                        url = (f"https://sofifa.com/players?type=all&ct%5B0%5D=2&r={offset}&set=true")
                    # Appending offset to url
                    url = url + (f"&offset={j*60}")
                    p_html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                    p_soup = p_html.text
                    data = Soup(p_soup,'html.parser')
                    table = data.find('tbody')

                    # Looping over objects 
                    for i in table.findAll('tr'):   
                        td = i.findAll('td')
                        # Get player id
                        ID = td[0].find('img').get('id')
                        # Get player name
                        Name = td[1].findAll('a')[0].text
                        # Get player age
                        Age = td[2].text.split()
                        # Get player rating
                        Overall_Rating = td[3].find('span').text
                        # Get player team
                        Team = td[5].find('a').text
                        # Get player contract time
                        Sub = td[5].find('div',{'class':'sub'}).text.strip()
                        # Get player wage
                        Wage = td[7].text.strip()
                        # Get player total states
                        Total_Stats = td[8].text.strip()
                        # Get info date
                        Date = data.findAll('span',{'class':'bp3-button-text'})[1].text
                        player_data = pd.DataFrame([[ID,Name,Age,Overall_Rating,Team,Sub,Wage,Total_Stats,Date]])
                        player_data.columns = column
                        # Appending data
                        df__scraped_players = pd.concat([df__scraped_players,player_data], ignore_index = True)
        
        self.log.info('Starting Teams Scraping ..')
        # Defining columns
        column_teams = ['ID','Team_Name']
        df_scraped_teams_data = pd.DataFrame(columns = column_teams)
        # Looping over pages
        for j in range (0,9):
            url = (f"https://sofifa.com/teams?type=all&ct%5B%5D=2&offset={j*60}")
            p_html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            p_soup = p_html.text
            data = Soup(p_soup,'html.parser')
            table = data.find('tbody')
            for i in table.findAll('tr'):   
                td = i.findAll('td')
                x = td[0].find('img').get('data-src')
                # Splitting to get id
                x = re.split('https://cdn.sofifa.net/teams/|/60.png',x)
                ID = x[1]
                # getting team name
                Team_Name = td[1].findAll('a')[0].text
                team_data = pd.DataFrame([[ID,Team_Name]])
                team_data.columns = column_teams
                # Appending data
                df_scraped_teams_data = pd.concat([df_scraped_teams_data,team_data], ignore_index = True)    
        # Exporting csv to relative path
        df_scraped_teams_data.to_csv('../scraped_teams.csv')
        df__scraped_players.to_csv('../scraped_players.csv')




