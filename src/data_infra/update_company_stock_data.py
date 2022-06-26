import requests
import csv
import io
import time
import pandas as pd
import yaml
import sqlalchemy as db#to save to db
import os #to get file path
import logging #to log



#time series data
def get_alpha_vantage_stock_time_series_data(function, ticker, outputsize, alpha_vantage_api_key):
    base_url = 'https://www.alphavantage.co/query?'
    params = {'function': function,
             'symbol': ticker,
             "datatype": 'csv',
             'outputsize': outputsize, #output size options: full, compact
             'apikey': alpha_vantage_api_key}
    response = requests.get(base_url, params=params)
    df = pd.read_csv(io.StringIO(response.text))
    df['ticker'] = ticker #create column with ticker
    df['timestamp'] = pd.to_datetime(df['timestamp'] , format="%Y-%m-%d", utc=True) #change format type
    df = df.rename(columns={'timestamp':'date'}) #standardize naming convention
    df = df.sort_values(by='date', ascending=True)

    return df




#### UPDATE COMPANY DATA SCRIPT ####

logging.basicConfig(filename= os.path.dirname(os.path.abspath(__file__)) + '/ingest_logs.log',
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


config_file_path = os.path.dirname(os.path.abspath(__file__)) + "/config.yml"

with open(config_file_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

alpha_vantage_api_key =  cfg["alphavantage"]["API_KEY"]

db_user = cfg["mysql"]["DB_USER"]
db_pass = cfg["mysql"]["DB_PASS"]
db_hostname = cfg["mysql"]["DB_HOSTNAME"]
db_port = cfg["mysql"]["DB_PORT"]
db_name = cfg["mysql"]["RAW_DATA_DB_NAME"]

db_ssl_ca_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/server-ca.pem'
db_ssl_cert_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-cert.pem'
db_ssl_key_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-key.pem'\


#https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661
engine = db.create_engine(
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

connection = engine.connect()
metadata = db.MetaData()


#connect to company_data_status table
company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

#query for first ticker that's not started
find_ticker_query = db.select([company_data_status]).where(company_data_status.columns.update_status == "NOT STARTED")
result = connection.execute(find_ticker_query).first()
ticker = result['ticker']

#update update_status for that specific ticker at hand
update_status_query = sqlalchemy.update(company_data_status).values(update_status = "IN PROGRESS").where(company_data_status.columns.ticker == ticker)
connection.execute(update_status_query)

#for ticker get company data and append it to company_stock table
company_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY',ticker, 'full', alpha_vantage_api_key )
company_stock_df.to_sql('company_stock', engine, if_exists='append', index=False, chunksize=500)

update_stock_query = sqlalchemy.update(company_data_status).values(stock_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
connection.execute(update_status_query)



# result = resultset.mappings().all()
# print(result)

#update company stock data
#def update_company_stock_data():


#update company adjusted stock data
#def update_company_adjusted_stock_data(ticker)


#update company earnings data

#update company



#query = db.update(emp).values(salary = 100000)
