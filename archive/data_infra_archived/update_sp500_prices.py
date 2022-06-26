import pandas_datareader as web # to get fred data
import pandas as pd
import datetime
import yaml #to open config file
import sqlalchemy #to save to db
import os #to get file path
import logging #to log


### FRED FUNCTIONS ###
#get sp500 price data from FRED #https://fred.stlouisfed.org/series/SP500
def get_sp500_prices():
    sp500_fred_start_date = datetime.datetime(2010, 9, 13).strftime("%Y-%m-%d") #FRED provides last 10 years of data; will need to loop back at this at some point
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df= web.DataReader(['SP500'], 'fred', sp500_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={"DATE":"date", "SP500":"sp500_close"})
    df['sp500_close'] = pd.to_numeric(df['sp500_close'], errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d', utc=True)

    return df

#script part
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


try:
    sp500_price = get_sp500_prices()
    #print(sp500_price)
    sp500_price.to_sql('sp500_prices', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: sp500_prices updated')
    print('SUCCESS: sp500_prices updated')
except Exception as e:
    logging.error("Can't update sp500_prices -- Error: ", e)
    print("Can't update sp500_prices -- Error: ", e)