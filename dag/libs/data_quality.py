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
import os.path
import os
import pandas as pd
import re
from airflow.models import BaseOperator
import requests
from airflow.utils.decorators import apply_defaults


"""Quality checking on inserted data.

    Args:
        postgres_user : user name for postgres login.
        postgres_password : password for the provided user name.
        postgres_host : database host.
        postgres_port : database port.
        postgres_database : database name.
        quality_checks : passing query and expected results to make quality checking more dynamic
"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,

                 postgres_user = "",
                 postgres_password = "",
                 postgres_host = "",
                 postgres_port = "",
                 postgres_database = "",
                 quality_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # Mapping parameters
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_database = postgres_database
        self.quality_checks = quality_checks

    def execute(self, context):
        # Connecting to postgres
        self.log.info('Connecting to Postgres')
        conn_string = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(self.postgres_user, self.postgres_password, self.postgres_host, self.postgres_port, self.postgres_database)
        engine = create_engine(conn_string, pool_size=5, pool_recycle=3600)
        postgres_conn = engine.connect()      
        
        self.log.info('Performing Quality Checks')
        for test in self.quality_checks:
            # Looping on queries and results and store them one by one 
            test_query = test.get('test_query')
            expected_result = test.get('expected_result')
            
            self.log.info('Executing Test Queries ..')
            # Executing query
            results = engine.execute(test_query)
            if expected_result != results.fetchall():
                raise ValueError(f"Test Failed .. Query {test_query}")
                
            else:
                self.log.info(f"Test Succeeded .. Query {test_query}")
