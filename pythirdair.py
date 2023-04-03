from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import os
import requests
import time
import json


# exercise: write a DAG which is able to request market data for a list of stocks.

# this list should be an input of your function. The functions names are left to help you

def get_data(**kwargs):
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=TSCO.LON&apikey=YSV4HVO7YT8G63WV'
    r = requests.get(url)
    data = r.json()
    print(data)

# DAG creatuion to call above  python logic
default_dag_args = { 'start_date': datetime(2022, 9, 1),
                     'email_on_failure': False,
                     'email_on_retry': False,
                     'retries': 1,
                     'retry_delay': timedelta(minutes=5),
                     'project_id': 1 }

# crontab notation can be useful https://crontab.guru/#0_0_*_*_1
with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:
    task_0 = PythonOperator(task_id="get_market_data", python_callable=get_data, op_kwargs={'tickers': []})


`


