import pandas_datareader as web # to get fred data
import pandas as pd
import datetime
import yaml #to open config file
import sqlalchemy #to save to db
import os #to get file path
import logging #to log

### FUNCTIONS ###
def get_historical_sp500_constituents():
    #data at https://github.com/fja05680/sp500
    start_date = datetime.datetime(1996, 1, 2).strftime("%Y-%m-%d") #start date from https://github.com/fja05680/sp500
    end_date = datetime.datetime(2021, 10, 27).strftime("%Y-%m-%d") #this potentially needs to be updated if the underlying csv updates
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True) #formatting

    df = pd.read_csv('data\sp500_constituents_history_through_10_27_2021.csv')
    df['date'] = pd.to_datetime(df['date'], utc=True) #formatting

    df = pd.merge(date_df, df, on='date', how='left')
    df = df.sort_values(by='date')
    df = df.fillna(method='ffill')

    return df

### SCRIPT ####
### THIS SCRIPT IS MEANT TO RUN ONCE A DAY ###
### SCRIPT ####
### THIS SCRIPT IS MEANT TO RUN ONCE A DAY ###
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

#dont really ever run this -- except if you want to reset everything -- so commenting the below to avoid running accidently
# try:
#     historical_sp500_constituents = get_historical_sp500_constituents()
#     print(historical_sp500_constituents)
#     historical_sp500_constituents.to_sql('sp500_constituents', engine, if_exists='replace', index=False, chunksize=500)
#     logging.info('SUCCESS: sp500_constituents uploaded')
#     print("SUCCESS: sp500_constituents uploaded")
# except Exception as e:
#     logging.error("Can't upload sp500_constituents -- Error: ", e)
#     print("ERROR: Can't upload sp500_constituentss")
