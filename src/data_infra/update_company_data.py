import requests
import csv
import io
import time
import pandas as pd
import yaml
import sqlalchemy #to save to db
import os #to get file path
import logging #to log


### WAYS TO IMPROVE ###
# 1.) only get and save data i care about --> reduce storage costs
# 2.) figure out how to only pull data on days when new data comes out



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

# secondary functions -- for convience
def get_av_quarterly_earnings(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('EARNINGS', ticker, 'quarterlyEarnings', alpha_vantage_api_key)
    df = df.rename(columns = {'reportedDate':'date'}) #standardize naming convention
    df['date'] = pd.to_datetime(df['date'] , format="%Y-%m-%d", utc=True) #change format time
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df = df.sort_values(by='date', ascending=True)
    return df

def get_av_quarterly_income_statements(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('INCOME_STATEMENT', ticker, 'quarterlyReports', alpha_vantage_api_key)
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df = df.sort_values(by='fiscalDateEnding', ascending=True)
    return df

def get_av_quarterly_balance_sheets(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('BALANCE_SHEET', ticker, 'quarterlyReports', alpha_vantage_api_key)
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df.loc[df['commonStockSharesOutstanding'] == 'None', 'commonStockSharesOutstanding'] = None
    df = df.sort_values(by='fiscalDateEnding', ascending=True)
    return df

def get_av_quarterly_cash_flow_statements(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('CASH_FLOW', ticker, 'quarterlyReports', alpha_vantage_api_key)
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df = df.sort_values(by='fiscalDateEnding', ascending=True)
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

#test functions
# earnings_df = get_av_quarterly_earnings('IBM', alpha_vantage_api_key)
# balance_sheet_df = get_av_quarterly_balance_sheets('IBM', alpha_vantage_api_key)
# adjusted_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY_ADJUSTED', 'IBM', 'full', alpha_vantage_api_key)
#
# earnings_df.to_csv('ibm_earnings.csv')
# balance_sheet_df.to_csv('ibm_balance_sheet.csv')
# adjusted_stock_df.to_csv('ibm_adjusted_stock.csv')


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

company_stock_frames = []
company_adjusted_stock_frames = []
company_earnings_frames = []
company_balance_sheet_frames = []
# company_income_statement_frames = []
# company_cash_flow_statement_frames = []
# company_overview_frames = []

count = 1
for ticker in tickers: #loop through all companies in the universe and get data

    try:
        company_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY',ticker, 'full', alpha_vantage_api_key )
        company_stock_frames.append(company_stock_df)  #append stock data to bigger frame
    except Exception as e:
        logging.error("No stock data for ", ticker," -- Error: ", e)
        print("No stock data for ", ticker," -- Error: ", e)

    time.sleep(20) #sleept b/c of api constraints -- 5 requests/min

    try:
        company_adjusted_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY_ADJUSTED', ticker, 'full', alpha_vantage_api_key)
        company_adjusted_stock_frames.append(company_adjusted_stock_df)
    except Exception as e:
        logging.error("No adjusted stock data for ", ticker," -- Error: ", e)
        print("No adjusted stock data for ", ticker," -- Error: ", e)


    time.sleep(20) #sleept b/c of api constraints -- 5 requests/min


    #get earnings data
    try:
        company_earnings_df = get_av_quarterly_earnings(ticker, alpha_vantage_api_key)
        company_earnings_frames.append(company_earnings_df)
    except Exception as e:
        logging.error("No earnings data for ", ticker," -- Error: ", e)
        print("No earnings  data for ", ticker," -- Error: ", e)

    time.sleep(20) #sleept b/c of api constraints -- 5 requests/min


    try:
        company_balance_sheet_df = get_av_quarterly_balance_sheets(ticker, alpha_vantage_api_key)
        company_balance_sheet_frames.append(company_balance_sheet_df)
    except Exception as e:
        logging.error("No balance sheet data for ", ticker," -- Error: ", e)
        print("No balance sheet data for ", ticker," -- Error: ", e)

    time.sleep(20) #sleept b/c of api constraints -- 5 requests/min


### WE DONT NEED OTHER DATA TODAY ###

    # try:
    #     company_income_statement_df = get_av_quarterly_income_statements(ticker, alpha_vantage_api_key)
    #     company_income_statement_frames.append(company_income_statement_df)
    # except Exception as e:
    #     logging.error("No income statement data for ", ticker," -- Error: ", e)
    #     print("No income statement data for ", ticker," -- Error: ", e)
    #
    # time.sleep(20)
    #
    #
    # try:
    #     company_cash_flow_statement_df = get_av_quarterly_cash_flow_statements(ticker, alpha_vantage_api_key)
    #     company_cash_flow_statement_frames.append(company_cash_flow_statement_df)
    # except Exception as e:
    #     logging.error("No cash flow statement data for ", ticker," -- Error: ", e)
    #     print("No cash flow statement data for ", ticker," -- Error: ", e)
    #
    # time.sleep(20)

    #for keeping track of how many companies you've looped through; will be important for cycluing out alpha vantage api keys
    print(count, ".) ", ticker, " updated")
    count = count + 1



# save data to database
try:
    company_stock_df = pd.concat(company_stock_frames, axis=0, ignore_index=True)
    company_stock_df.to_sql('company_stock', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_stock updated')
    print("SUCCESS: company_stock updated")
except Exception as e:
    logging.error("Can't update company_stock -- Error: ", e)
    print("ERROR: Can't update company_stock")

try:
    company_adjusted_stock_df = pd.concat(company_adjusted_stock_frames, axis=0, ignore_index=True)
    company_adjusted_stock_df.to_sql('company_adjusted_stock', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_adjusted_stock updated')
    print("SUCCESS: company_adjusted_stock updated")
except Exception as e:
    logging.error("Can't update company_adjusted_stock -- Error: ", e)
    print("ERROR: Can't update company_adjusted_stock")

try:
    company_earnings_df = pd.concat(company_earnings_frames, axis=0, ignore_index=True)
    company_earnings_df.to_sql('company_earnings', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_earnings updated')
    print("SUCCESS: company_earnings updated")
except Exception as e:
    logging.error("Can't update company_earnings -- Error: ", e)
    print("ERROR: Can't update company_earnings")

try:
    company_balance_sheet_df = pd.concat(company_balance_sheet_frames, axis=0, ignore_index=True)
    company_balance_sheet_df.to_sql('company_balance_sheet', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_balance_sheet updated')
    print("SUCCESS: company_balance_sheet updated")
except Exception as e:
    logging.error("Can't update company_balance_sheet -- Error: ", e)
    print("ERROR: Can't update company_balance_sheet")

# try:
#     company_income_statement_df = pd.concat(company_income_statement_frames, axis=0, ignore_index=True)
#     company_income_statement_df.to_sql('company_income_statement', engine, if_exists='replace', index=False, chunksize=500)
#     logging.info('SUCCESS: company_income_statement updated')
#     print("SUCCESS: company_income_statement updated")
# except Exception as e:
#     logging.error("Can't update company_income_statement -- Error: ", e)
#     print("ERROR: Can't update company_income_statement")
#
# try:
#     company_cash_flow_statement_df = pd.concat(company_cash_flow_statement_frames, axis=0, ignore_index=True)
#     company_cash_flow_statement_df.to_sql('company_cash_flow_statement', engine, if_exists='replace', index=False, chunksize=500)
#     logging.info('SUCCESS: company_cash_flow_statement updated')
#     print("SUCCESS: company_cash_flow_statement updated")
# except Exception as e:
#     logging.error("Can't update company_cash_flow_statement -- Error: ", e)
#     print("ERROR: Can't update company_cash_flow_statement")
