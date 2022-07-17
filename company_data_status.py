import requests
import csv
import io
import time
import pandas as pd
import numpy as np
import yaml
import sqlalchemy #to save to db
import os #to get file path
import logging #to log




#### UPDATE COMPANY DATA SCRIPT ####

logging.basicConfig(filename= os.path.dirname(os.path.abspath(__file__)) + '/ingest_logs.log',
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


config_file_path = os.path.dirname(os.path.abspath(__file__)) + "/config.yml"

with open(config_file_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

db_user = cfg["mysql"]["DB_USER"]
db_pass = cfg["mysql"]["DB_PASS"]
db_hostname = cfg["mysql"]["DB_HOSTNAME"]
db_port = cfg["mysql"]["DB_PORT"]
db_name = cfg["mysql"]["RAW_DATA_DB_NAME"]

db_ssl_ca_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/server-ca.pem'
db_ssl_cert_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-cert.pem'
db_ssl_key_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-key.pem'\


#https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661
engine = sqlalchemy.create_engine(
     sqlalchemy.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=db_user,  # e.g. "my-database-user"
        password=db_pass,  # e.g. "my-database-password"
        host=db_hostname,  # e.g. "127.0.0.1"
        port=db_port,  # e.g. 3306
        database=db_name,  # e.g. "my-database-name"
    ),
    connect_args = {
        'ssl_ca': db_ssl_ca_path ,
        'ssl_cert': db_ssl_cert_path,
        'ssl_key': db_ssl_key_path
    }
)



#create table that will be used to facilate daily update of all underlying company data

#for today our universe of companies is the sp500
df = pd.read_sql_table('sp500_constituents', engine) #read in current sp500_constituents table
df = df.loc[df['date'] >= '2021-01-01'] #filter for only the dates you want; will delete later

#get unique tickers
tickers = df['tickers'].str.cat(sep=',') #add all tickers into one string
tickers = tickers.split(",") #create list of all SP_500_companies
tickers = list(set(tickers)) #find all unique companies
tickers = sorted(tickers) #sort company list alphabetically

#convert list of unique tickers into dataframe
df = pd.DataFrame (tickers, columns = ['ticker'])

#add columns to dataframe
df["update_status"] = "NOT STARTED"

df["stock_update_status"] = "NOT STARTED"
df["last_stock_update_date"] = "#N/A"

df["adjusted_stock_update_status"] = "NOT STARTED"
df["last_adjusted_stock_update_date"] = "N/A"

df["earnings_update_status"] = "NOT STARTED"
df["last_earnings_update_date"] = "N/A"

df["balance_sheet_update_status"] = "NOT STARTED"
df["last_balance_sheet_update_date"] = "N/A"


#FOR TESTING only
df = df.head(10)

#save df to database
try:

    #create table that will keep track of daily updates for companies
    df.to_sql('company_data_status', engine, if_exists='replace', index=False, chunksize=500) #replace existing company status page

    #replace existing company stock tables w/ empty table
    company_stock_df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker'])
    company_stock_df.to_sql('company_stock', engine, if_exists='replace', index=False, chunksize=500)

    #replace existing adjusted company stock tables w/ empty table
    company_adjusted_stock_df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient', 'ticker'])
    company_adjusted_stock_df.to_sql('company_adjusted_stock', engine, if_exists='replace', index=False, chunksize=500)

    #replace existing company earnings tables w/ empty table
    company_earnings_df = pd.DataFrame(columns = ['fiscalDateEnding', 'date', 'reportedEPS', 'estimatedEPS', 'surprise', 'surprisePercentage', 'ticker'])
    company_earnings_df.to_sql('company_earnings', engine, if_exists='replace', index=False, chunksize=500)

    #replace existing company earnings tables w/ empty table
    company_balance_sheet_df = pd.DataFrame(columns = ['fiscalDateEnding','reportedCurrency','totalAssets','totalLiabilities','totalShareholderEquity','commonStock','commonStockSharesOutstanding','cash','ticker'])
    company_balance_sheet_df.to_sql('company_balance_sheet', engine, if_exists='replace', index=False, chunksize=500)

    logging.info('SUCCESS: company_data_status created')
    print("SUCCESS: company_data_status created")
except Exception as e:
    logging.error("Can't create company_data_status -- Error: ", e)
    print("Can't create company_data_status -- Error: ", e)
