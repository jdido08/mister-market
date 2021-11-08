import requests
import csv
import io
import time
import pandas as pd
import yaml
import sqlalchemy #to save to db
import os #to get file path
import logging #to log


### FUNCTIONS TO GET DATA FROM ALPHA VANTAGE ####
#fundamental data
def get_alpha_vantage_fundamental_data(function, ticker, json_object, alpha_vantage_api_key):
    base_url = 'https://www.alphavantage.co/query?'
    params = {'function': function,
             'symbol': ticker,
             "datatype": 'json',
             'apikey': alpha_vantage_api_key}
    response = requests.get(base_url, params=params)
    response = response.json()
    if(json_object != None):
        response = response[json_object]
    df = pd.json_normalize(response)
    df['ticker'] = ticker #create column with ticker
    return df

# secondary functions -- for convience
def get_av_company_overview(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('OVERVIEW', ticker, None, alpha_vantage_api_key)
    df = df.rename(columns = {'52WeekHigh':'_52WeekHigh',
                              '52WeekLow':'_52WeekLow',
                              '50DayMovingAverage':'_50DayMovingAverage',
                              '200DayMovingAverage':'_200DayMovingAverage'}) #cant start field name with number
    return df



#### UPDATE COMPANY SCRIPT ####

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



#company, date table

#for today our universe of companies is the sp500
df = pd.read_sql_table('sp500_constituents', engine) #read in current sp500_constituents table
df = df.loc[df['date'] >= '2021-01-01'] #filter for only the dates you want; will delete later

tickers = df['tickers'].str.cat(sep=',') #add all tickers into one string
tickers = tickers.split(",") #create list of all P_500_companies
tickers = list(set(tickers)) #find all unique companies
tickers = sorted(tickers) #sort company list alphabetically

#for testing purposes only!
tickers = tickers[0:5] #get first 10 tickers
#tickers = ["ABC"]

company_overview_frames = []

count = 1
for ticker in tickers: #loop through all companies in the universe and get data

    try:
        company_overview_df = get_av_company_overview(ticker, alpha_vantage_api_key)
        company_overview_frames.append(company_overview_df)
    except Exception as e:
        logging.error("No overview data for ", ticker," -- Error: ", e)
        print("No overview data for ", ticker," -- Error: ", e)

    time.sleep(20) #sleept b/c of api constraints -- 5 requests/min

    #for keeping track of how many companies you've looped through; will be important for cycluing out alpha vantage api keys
    print(count, ".) ", ticker, " updated")
    count = count + 1


try:
    company_overview_df =  pd.concat(company_overview_frames, axis=0, ignore_index=True)
    company_overview_df.to_sql('company_overview', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_overview updated')
    print("SUCCESS: company_overview updated")
except Exception as e:
    logging.error("Can't update company_overview -- Error: ", e)
    print("ERROR: Can't update company_overview")
