from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from libs.data_scraping import (DataScraping)
from libs.data_loading_trasforming import (DataLoadingAndTransforming)
from libs.data_quality import (DataQualityOperator)
import pandas as pd


# Passing args for the dag 
default_args = {
    'owner': 'eman.hamied',
    'start_date': datetime(2023, 2, 2),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


# Dag creation
dag = DAG('fifa_dag',
          default_args=default_args,
          description='Scrape, Transform and Load data using Airflow',
          schedule_interval=None
          
        )

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)


# Data Scraping task
data_scraping = DataScraping(
    task_id='Data_Scraping',
    dag=dag
)


# Data Loading and Transformin task
data_loading_transforming = DataLoadingAndTransforming(
    task_id='Data_Loading_Transforming',
    dag=dag,
    postgres_user="postgres",
    postgres_password="[your_passwor]",
    postgres_host="127.0.0.1",
    postgres_port="5432",
    postgres_database="european_soccer"
)


# Quality checking task 
data_quality = DataQualityOperator(
    task_id='Data_Quality_Checks',
    dag=dag,
    postgres_user="postgres",
    postgres_password="[your_passwor]",
    postgres_host="127.0.0.1",
    postgres_port="5432",
    postgres_database="european_soccer",
    quality_checks=[{'test_query':'SELECT count(*) FROM dim_player WHERE player_name = NULL','expected_result': 0},
                    {'test_query':'SELECT count(*) FROM fct_match fm WHERE match_id = NULL ','expected_result': 0},
                    {'test_query':'SELECT count(DISTINCT league_id) FROM dim_league','expected_result':11}]
)


end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

# Mapping operators 

start_operator >> data_scraping >> data_loading_transforming >> data_quality >> end_operator
