import requests
import csv
import io
import time
import pandas as pd
import yaml
import sqlalchemy #to save to db
import os #to get file path
import logging #to log


def get_alpha_vantage_release_calendar_data(function, horizon, alpha_vantage_api_key):
    base_url = 'https://www.alphavantage.co/query?'
    params = {'function': function,
             'datatype': 'csv',
             'horizon': horizon,
             'apikey': alpha_vantage_api_key}
    response = requests.get(base_url, params=params)
    df = pd.read_csv(io.StringIO(response.text))
    df['reportDate'] = pd.to_datetime(df['reportDate'] , format="%Y-%m-%d", utc=True) #change format type
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True)
    df = df.rename(columns={'reportDate':'date'}) #standardize naming convention
    df = df.rename(columns={'symbol':'ticker'})
    print(df.dtypes)
    return df

## update release calendar script ####
logging.basicConfig(filename= os.path.dirname(os.path.abspath(__file__)) + '/ingest_logs.log',
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


config_file_path = os.path.dirname(os.path.abspath(__file__)) + "/config.yml"

with open(config_file_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

alpha_vantage_api_key =  cfg["alphavantage"]["API_KEY"]


df = get_alpha_vantage_release_calendar_data('EARNINGS_CALENDAR', '12month', alpha_vantage_api_key)
print(df)
