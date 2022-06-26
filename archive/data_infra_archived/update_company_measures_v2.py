import pandas as pd
import numpy as np
import yaml
import sqlalchemy #to save to db
import os #to get file path
import logging #to log
from functools import reduce

### FUNCTIONS ###

def get_shares_outstanding(report_date_df, bs_df, adjusted_stock_df):
    # *****  this is wrong!! earnings release does not necessary means report date ****#
    shares_df = pd.merge(report_date_df, bs_df, on=['ticker','last_reported_quarter'], how='left') ### THERES an implicit assumption here that earnings release day = quarterlyReports day which is false
    shares_df = pd.merge(adjusted_stock_df, shares_df, on=['date','ticker'], how='left')

    shares_df['last_reported_shares_outstanding'] = shares_df['last_reported_shares_outstanding'].fillna(method='ffill')
    shares_df['last_reported_quarter'] = shares_df['last_reported_quarter'].fillna(method='ffill')
    shares_df['split_coefficient'] = shares_df['split_coefficient'].fillna(value=1.0)

    shares_df['last_reported_shares_outstanding'] = shares_df['last_reported_shares_outstanding'].astype(float)
    shares_df['split_coefficient'] = shares_df['split_coefficient'].astype(float)

    #calc shares outstanding for every day
    shares_df['quarterly_rolling_split_coefficient'] = shares_df.groupby('last_reported_quarter')['split_coefficient'].cumprod()
    shares_df['shares_outstanding'] = shares_df['last_reported_shares_outstanding'] * shares_df['quarterly_rolling_split_coefficient']

    #prep output
    shares_df['shares_outstanding'] = shares_df['shares_outstanding'].astype(float)
    #shares_df['shares_outstanding'] = shares_df['shares_outstanding'].fillna(method='ffill') #maybe not necessary
    shares_df = shares_df[['ticker','date','shares_outstanding']]
    return shares_df

def get_dividends(dividend_df,shares_df):

    #merge dividend and shares count
    dividend_df = pd.merge(dividend_df, shares_df, on=['ticker','date'], how='left')

    #create a dataframe for dates so when i compute dividend ttm is accurate
    start_date = dividend_df['date'].min()
    end_date = dividend_df['date'].max()
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True).dt.strftime("%Y-%m-%d")
    date_df = date_df.sort_values(by='date', ascending=True)

    #merge dataframes
    dividend_df = pd.merge(date_df, dividend_df, on=['date'], how='left').reset_index()

    #massage dataframe
    dividend_df = dividend_df.rename(columns={'dividend_amount':'dps'})
    dividend_df['dps'] = dividend_df['dps'].astype(float)
    dividend_df['shares_outstanding'] = dividend_df['shares_outstanding'].astype(float)
    dividend_df['dps'] = dividend_df['dps'].fillna(value=0.0) #fill in all empty dps as zero
    dividend_df['shares_outstanding'] = dividend_df['shares_outstanding'].fillna(method='ffill') #just in case pull forward any missing share amounts

    #calc dps_ttm
    dividend_df['dividends'] = dividend_df['dps'] * dividend_df['shares_outstanding']  #calc dividends total amount (not per share)
    dividend_df['dividends_ttm'] = dividend_df['dividends'].rolling(365).apply(sum, raw=True)
    dividend_df['dps_ttm'] = dividend_df['dividends_ttm'] / dividend_df['shares_outstanding']

    #prep dataframe
    dividend_df['ticker'] = dividend_df['ticker'].fillna(value=ticker) #formatting
    dividend_df = dividend_df[['date', 'ticker', 'dps_ttm','dividends_ttm']]

    return dividend_df

#need to fix
def get_earnings(earnings_df, shares_df):

    #merge earnings and shares count
    earnings_df = pd.merge(earnings_df, shares_df, on=['ticker','date'], how='left').reset_index()
    earnings_df['shares_outstanding'] = earnings_df['shares_outstanding'].astype(float)
    earnings_df['shares_outstanding'] = earnings_df['shares_outstanding'].fillna(method='ffill') #just in case there is missing shares data fill forward

    #calc earnings and earnings ttm
    earnings_df['non_gaap_earnings'] = earnings_df['non_gaap_eps'] * earnings_df['shares_outstanding']
    earnings_df['non_gaap_earnings_ttm'] = earnings_df['non_gaap_earnings'].rolling(4).apply(sum, raw=True)
    earnings_df = earnings_df[['date', 'non_gaap_earnings_ttm']]  #need to drop columns before remerging shares database

    #create a dataframe for dates so when i compute dividend ttm is accurate
    start_date = shares_df['date'].min()
    end_date = shares_df['date'].max()
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True).dt.strftime("%Y-%m-%d")
    date_df = date_df.sort_values(by='date', ascending=True)

    #merge dataframes -- need to merge dates first
    earnings_df = pd.merge(date_df, earnings_df, on=['date'], how='left').reset_index()


    #remerge shares data
    earnings_df = pd.merge(earnings_df, shares_df, on=['date'], how = 'left').reset_index()
    earnings_df['non_gaap_earnings_ttm'] = earnings_df['non_gaap_earnings_ttm'].fillna(method='ffill') #fill forward missing earnings
    earnings_df['shares_outstanding'] = earnings_df['shares_outstanding'].fillna(method='ffill') #shouldnt be any missing but just in case

    #calc eps
    earnings_df['non_gaap_eps_ttm'] = earnings_df['non_gaap_earnings_ttm'] / earnings_df['shares_outstanding']

    #prep dataframe
    earnings_df['ticker'] = earnings_df['ticker'].fillna(value=ticker) #formatting
    earnings_df = earnings_df[['date', 'ticker', 'non_gaap_eps_ttm', 'non_gaap_earnings_ttm']]

    return earnings_df



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



#company, date table

#for today our universe of companies is the sp500
df = pd.read_sql_table('sp500_constituents', engine) #read in current sp500_constituents table)
df = df.loc[df['date'] >= np.datetime64(cfg["startdate"]) ]#filter for only the dates you want; will delete later

tickers = df['tickers'].str.cat(sep=',') #add all tickers into one string
tickers = tickers.split(",") #create list of all P_500_companies
tickers = list(set(tickers)) #find all unique companies
tickers = sorted(tickers) #sort company list alphabetically

#for testing purposes only!
#tickers = tickers[0:5] #get first 10 tickers
#tickers = ["ABC"]

#get raw price
price_df_sql_query = "SELECT * FROM company_stock WHERE date > " +  cfg["startdate"].strftime("%Y-%m-%d")
price_df = pd.read_sql_query(price_df_sql_query, engine)
price_df = price_df[['date','ticker','close']]
price_df['date'] = pd.to_datetime(price_df['date'], utc=True).dt.strftime("%Y-%m-%d")
price_df = price_df.sort_values(by='date', ascending=True)
price_df = price_df.rename(columns={'close':'close_price'})
print("print_df read")


#get adjusted price
adjusted_stock_df_sql_query = "SELECT * FROM company_adjusted_stock WHERE date > " + cfg["startdate"].strftime("%Y-%m-%d")
adjusted_stock_df = pd.read_sql_query(adjusted_stock_df_sql_query, engine)


adjusted_price_df = adjusted_stock_df[['date','ticker','adjusted_close']].copy()
adjusted_price_df['date'] = pd.to_datetime(adjusted_price_df['date'], utc=True).dt.strftime("%Y-%m-%d")
adjusted_price_df = adjusted_price_df.sort_values(by='date', ascending=True)
adjusted_price_df = adjusted_price_df.rename(columns={'adjusted_close':'adjusted_close_price'})
print("adjusted_price_df read")

dividend_df = adjusted_stock_df[['ticker','date','dividend_amount']].copy()
dividend_df[dividend_df['dividend_amount'] == 'None'] = None
dividend_df['date'] = pd.to_datetime(dividend_df['date'], utc=True).dt.strftime("%Y-%m-%d")
dividend_df = dividend_df.sort_values(by='date', ascending=True)
print("adjusted_dividend_df read")


adjusted_stock_df  = adjusted_stock_df[['date','ticker','split_coefficient']].copy()
#adjusted_stock_df = adjusted_stock_df[adjusted_stock_df['date'] > '2017-01-01']
adjusted_stock_df['date'] = pd.to_datetime(adjusted_stock_df['date'], utc=True).dt.strftime("%Y-%m-%d")
adjusted_stock_df = adjusted_stock_df.sort_values(by='date', ascending=True)
print("adjusted_stock_df read")

# *****  this is wrong!! earnings release does not necessary means report date ****#
report_date_df = pd.read_sql_table('company_earnings', engine)
report_date_df = report_date_df[['date', 'ticker','fiscalDateEnding']]
report_date_df['date'] = pd.to_datetime(report_date_df['date'], utc=True).dt.strftime("%Y-%m-%d")
report_date_df = report_date_df.sort_values(by='date', ascending=True)
report_date_df = report_date_df.rename(columns={'fiscalDateEnding':'last_reported_quarter'})
print("report_date_df read")

bs_df = pd.read_sql_table('company_balance_sheet', engine)
bs_df = bs_df[['ticker','fiscalDateEnding','commonStockSharesOutstanding']]
bs_df[bs_df['commonStockSharesOutstanding'] == 'None'] = None
bs_df = bs_df.rename(columns={'commonStockSharesOutstanding':'last_reported_shares_outstanding','fiscalDateEnding':'last_reported_quarter'})
print("bs_df read")


#get company earnings data
earnings_df = pd.read_sql_table('company_earnings', engine)
earnings_df = earnings_df[['date', 'ticker', 'reportedEPS']]
earnings_df['date'] = pd.to_datetime(earnings_df['date'], utc=True).dt.strftime("%Y-%m-%d")
earnings_df = earnings_df.sort_values(by='date', ascending=True)
earnings_df = earnings_df.rename(columns={'reportedEPS':'non_gaap_eps'})
earnings_df[earnings_df['non_gaap_eps'] == 'None'] = None
earnings_df['non_gaap_eps'] = earnings_df['non_gaap_eps'].astype(float)
print("earnings_df read")


#dont need to upload this here
# meta_data_df = pd.read_sql_table('company_overview', engine)
# meta_data_df = meta_data_df[['ticker','Sector','Industry', 'Exchange','Currency','Name']]
# meta_data_df = meta_data_df.rename(columns={"Sector":"sector", "Industry":"industry","Currency":"currency","Name":"name", "Exchange":"exchange"}) #style thing much make lowe case some column names
# print("meta_data_df read")


measures_frames = []

count = 1
total_count = 1
for ticker in tickers: ### THIS IS SUCH A BAD WAY TO DO THIS BUT JUST MOVING FORWARD ANYHOW ###
    try:

        company_price_df = price_df[price_df['ticker'] == ticker]
        company_adjusted_price_df = adjusted_price_df[adjusted_price_df['ticker'] == ticker]
        company_report_date_df = report_date_df[report_date_df['ticker'] == ticker]
        company_bs_df = bs_df[bs_df['ticker'] == ticker]
        company_adjusted_stock_df = adjusted_stock_df[adjusted_stock_df['ticker'] == ticker]
        company_dividend_df = dividend_df[dividend_df['ticker'] == ticker]
        company_earnings_df = earnings_df[earnings_df['ticker'] == ticker]
        # company_meta_data_df = meta_data_df[meta_data_df['ticker'] == ticker] #dont need this here


        company_price_df = pd.merge(company_price_df, company_adjusted_price_df, on=['date','ticker'], how='left')
        company_shares_df = get_shares_outstanding(company_report_date_df,company_bs_df,company_adjusted_stock_df)
        company_dividend_df = get_dividends(company_dividend_df, company_shares_df)
        company_earnings_df = get_earnings(company_earnings_df, company_shares_df)


        #date, ticket, sector, industry, exchange, currency, country, price, ttm dividends, ttm earnings, shares, marketcap, payout ratio
        data_frames = [company_price_df, company_shares_df, company_dividend_df, company_earnings_df]
        company_measures_df = reduce(lambda  left,right: pd.merge(left,right,on=['date','ticker'], how='left'), data_frames)
        #company_measures_df = pd.merge(company_measures_df, company_meta_data_df, on='ticker', how='left')

        #compute additional meaures
        company_measures_df['marketcap'] = company_measures_df['close_price'] * company_measures_df['shares_outstanding']
        company_measures_df['payout_ratio'] = company_measures_df['dps_ttm'] / company_measures_df['non_gaap_eps_ttm']
        company_measures_df['dividend_yield'] = company_measures_df['dps_ttm'] / company_measures_df['close_price']
        company_measures_df['earnings_yield'] = company_measures_df['non_gaap_eps_ttm'] / company_measures_df['close_price']

        #append
        measures_frames.append(company_measures_df)
        #print("success processed measure for: ", ticker)

    except:
        count = count + 1
        print("Can't update ", count, " .) ", ticker)

    total_count = total_count + 1

#create tables for each company fundamental
try:
    measures_df = pd.concat(measures_frames, axis=0, ignore_index=True)
    measures_df.to_sql('company_measures', engine, if_exists='replace', index=False, chunksize=500)
    #logging.info('SUCCESS: company_measures updated')
    print("SUCCESS: company_measures updated")
    print("total count: ", total_count)
except Exception as e:
    #logging.error("Can't update company_measures -- Error: ", e)
    print("ERROR: Can't update company_measures")
