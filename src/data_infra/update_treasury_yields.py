import pandas_datareader as web # to get fred data
import pandas as pd
import datetime
import yaml #to open config file
import sqlalchemy #to save to db
import os #to get file path
import logging #to log


logging.basicConfig(filename= os.path.dirname(os.path.abspath(__file__)) + '/ingest_logs.log',
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


### FRED FUNCTIONS ###
def get_treasury_yields(): #get treasry yields functions
    treasury_fred_start_date = datetime.datetime(2000, 1, 1).strftime("%Y-%m-%d") #rates go back to 1962 but just starting with 2000
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df = web.DataReader(['DGS1', 'DGS2','DGS3','DGS5','DGS7','DGS10','DGS20','DGS30'], 'fred',treasury_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={
        "DATE":"date",
        "DGS1":"_1y",
        "DGS2":"_2y",
        "DGS3":"_3y",
        "DGS5":"_5y",
        "DGS7":"_7y",
        "DGS10":"_10y",
        "DGS20":"_20y",
        "DGS30":"_30y"})
    df = df.sort_values(by='date')
    column_names = ['_1y','_2y','_3y','_5y','_7y','_10y','_20y','_30y']
    df[column_names] = df[column_names].apply(pd.to_numeric, errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], utc=True)

    return df


#update script
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
    treasury_yields = get_treasury_yields()
    #print(treasury_yields)
    treasury_yields.to_sql('treasury_yields', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: treasury_yields updated')
    print("SUCCESS: treasury_yields updated")
except:
    logging.error("Can't update treasury_yield -- Error: ", e)
    print("ERROR: Can't update treasury_yields")
