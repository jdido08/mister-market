import requests
import csv
import io
import time
import pandas as pd
import yaml
import sqlalchemy as db #to save to db
import os #to get file path
import logging #to log
from datetime import datetime


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
    print(df)
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
    df = df[['fiscalDateEnding','reportedCurrency','totalAssets','totalLiabilities','totalShareholderEquity','commonStock','commonStockSharesOutstanding','ticker']]
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

#alpha_vantage_api_key =  cfg["alphavantage"]["API_KEY"]
alpha_vantage_api_key =  cfg["alphavantage"]["SPECIAL_KEY"]

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
     db.engine.url.URL.create(
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




#update company stock data
def update_company_stock_data():
    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    #query for first ticker that's not started
    find_ticker_query = db.select([company_data_status]).where(company_data_status.columns.update_status == "NOT STARTED")
    result = connection.execute(find_ticker_query).first()


    if(result):
        ticker = result['ticker']

        try:
            #update update_status for that specific ticker at hand
            update_status_query = db.update(company_data_status).values(update_status = "IN PROGRESS").where(company_data_status.columns.ticker == ticker)
            connection.execute(update_status_query)

            #for ticker get company data and append it to company_stock table
            company_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY', ticker, 'full', alpha_vantage_api_key )
            company_stock_df.to_sql('company_stock', engine, if_exists='append', index=False, chunksize=500)
            time.sleep(2)

            #set status of query
            update_status_query = db.update(company_data_status).values(stock_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
            logging.info('SUCCESS: %s stock data updated', ticker)
            print('SUCCESS: ', ticker, ' stock data updated')

        except Exception as e:
            #set status of query
            update_status_query = db.update(company_data_status).values(stock_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
            logging.error('ERROR: Cant update company stock data -- Error: ', e)
            print('ERROR: Cant update ', ticker, ' stock data')


        #update status
        connection.execute(update_status_query)
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        update_date_query = db.update(company_data_status).values(last_stock_update_date = now).where(company_data_status.columns.ticker == ticker)
        connection.execute(update_date_query)

        update_company_adjusted_stock_data(ticker)

    else:
        print("NOTHING ELSE TO UPDATE!")


#update company adjusted stock data
def update_company_adjusted_stock_data(ticker):
    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:

        # *** SKIPPING THIS FOR NOW BECAUSE I NEED A PREMIUM API KEY TO ACCESS THIS DATA ***

        #company_adjusted_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY_ADJUSTED', ticker, 'full', alpha_vantage_api_key)
        #company_adjusted_stock_df.to_sql('company_adjusted_stock', engine, if_exists='append', index=False, chunksize=500)
        #time.sleep(5)

        #set status update
        company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)
        update_status_query = db.update(company_data_status).values(adjusted_stock_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        logging.info('SUCCESS: %s adjusted stock data updated', ticker)
        print('SUCCESS: ', ticker, ' adjusted stock data updated')

    except Exception as e:
        update_status_query = db.update(company_data_status).values(adjusted_stock_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        logging.error('Cant update company adjusted stock data -- Error: ', e)
        print('ERROR: Cant update ', ticker, 'company adjust stock data')


    #update status
    connection.execute(update_status_query)
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_adjusted_stock_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)

    update_company_earnings_data(ticker)


#update company earnings data
def update_company_earnings_data(ticker):
    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:
        company_earnings_df = get_av_quarterly_earnings(ticker, alpha_vantage_api_key)
        company_earnings_df.to_sql('company_earnings', engine, if_exists='append', index=False, chunksize=500)
        time.sleep(2)

        #connect to company_data_status table
        company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)
        update_status_query = db.update(company_data_status).values(earnings_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        logging.info('SUCCESS: %s earnings data updated', ticker)
        print('SUCCESS: ', ticker, ' earnings data updated')

    except Exception as e:
        update_status_query = db.update(company_data_status).values(earnings_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        logging.error('Cant update company earnings data -- Error: ', e)
        print('ERROR: Cant update ', ticker, ' company earnings data')
        time.sleep(2)

    #make update
    connection.execute(update_status_query)
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_earnings_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)

    update_company_balance_sheet_data(ticker)

#update company
def update_company_balance_sheet_data(ticker):

    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:
        company_balance_sheet_df = get_av_quarterly_balance_sheets(ticker, alpha_vantage_api_key)
        company_balance_sheet_df.to_sql('company_balance_sheet', engine, if_exists='append', index=False, chunksize=500)
        time.sleep(2)

        #connect to company_data_status table
        company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)
        update_status_query = db.update(company_data_status).values(balance_sheet_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        logging.info('SUCCESS: %s balance sheet data updated', ticker)
        print('SUCCESS: ', ticker, ' balance sheet data updated')

    except Exception as e:
        update_status_query = db.update(company_data_status).values(balance_sheet_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        logging.error('Cant update company balance sheet data-- Error: ', e)
        print('ERROR: Cant update ',ticker, ' balance sheet data')
        time.sleep(2)

    #make update
    connection.execute(update_status_query)
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_balance_sheet_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)

    #update update_status for that specific ticker at hand
    update_status_query = db.update(company_data_status).values(update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
    connection.execute(update_status_query)

    update_company_stock_data()

df = pd.read_sql_table('company_data_status', engine) #read in current sp500_constituents table
print(df)
update_company_stock_data()

df = pd.read_sql_table('company_data_status', engine) #read in current sp500_constituents table
print(df)
df.to_csv("company_status_2.csv")


# ticker = "AAL"
# company_balance_sheet_df = get_av_quarterly_balance_sheets(ticker, alpha_vantage_api_key)
# print(company_balance_sheet_df)
# company_balance_sheet_df.to_csv("aal_bs")


#elephant graveyard
#query = db.update(emp).values(salary = 100000)
#for today our universe of companies is the sp500
#df = pd.read_sql_table('sp500_constituents', engine) #read in current sp500_constituents table

#company_data_status_table = sqlalchemy.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

# query = sqlalchemy.select([company_data_status_table]).where(company_data_status_table.columns.update_status == "NOT STARTED")
# result = connection.execute(query).first()
# result = result['ticker']
# print(result)
#
# query = sqlalchemy.update(company_data_status_table).values(update_status = "IN PROGRESS").where(company_data_status_table.columns.ticker == result)
# result = connection.execute(query)
#
# df = pd.read_sql_table('company_data_status', engine) #read in current sp500_constituents table
# print(df)

# result = resultset.mappings().all()
# print(result)
