import pandas_datareader as web
import pandas as pd
import datetime
import yaml
import sqlalchemy
import sys
import os



### FRED FUNCTIONS ###

#get sp500 price data from FRED #https://fred.stlouisfed.org/series/SP500
def get_sp500_prices():
    sp500_fred_start_date = datetime.datetime(2010, 9, 13).strftime("%Y-%m-%d") #start date on FRED wesbite; I have a csv of historical data if I ever need it
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df= web.DataReader(['SP500'], 'fred', sp500_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={"DATE":"date", "SP500":"sp500_close"})
    df['sp500_close'] = pd.to_numeric(df['sp500_close'], errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d', utc=True)

    return df

# print(sys.path[0])
# print(os.path.abspath(os.getcwd()))
# print(os.getcwd())
# print(os.path.dirname(os.path.abspath(__file__)))

with open("config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

db_user = cfg["mysql"]["DB_USER"]
db_pass = cfg["mysql"]["DB_PASS"]
db_hostname = cfg["mysql"]["DB_HOSTNAME"]
db_port = cfg["mysql"]["DB_PORT"]
db_name = cfg["mysql"]["RAW_DATA_DB_NAME"]

db_ssl_ca_path = os.path.abspath(os.getcwd()) + '/ssl/server-ca.pem'
db_ssl_cert_path = os.path.abspath(os.getcwd()) + '/ssl/client-cert.pem'
db_ssl_key_path = os.path.abspath(os.getcwd()) + '/ssl/client-key.pem'\

print(db_ssl_ca_path)
print(db_ssl_cert_path)
print(db_ssl_key_path)

https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661
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
    print('get sp500 sucesss')
    sp500_price.to_sql('sp500_prices', engine, if_exists='replace', index=False, chunksize=500)
    print("SUCCESS: sp500_prices updated")
except Exception as e:
    print("Can't update sp500_prices -- Error: ", e)
