import sqlalchemy as db #to save to db
import pandas as pd
from google.cloud import secretmanager # Import the Secret Manager client library.
import google_crc32c
import os #to get file path
from functools import reduce

################################################################################
######################## SUPPORTING INFRASTRUCTURE #############################
################################################################################

def get_secret(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    #for local dev -- set google app credentials
    google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-6e485429eb5e.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path

    #link: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
    #follow instruction here to run locally: https://cloud.google.com/docs/authentication/production#create-service-account-gcloud



    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return response
    else:
        payload = response.payload.data.decode("UTF-8")
        return payload


connection_name = "mister-market-project:us-central1:mister-market-db"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
db_user = "root"
db_name = "raw_data"
db_password = get_secret("mister-market-project", "db_password", "1")
db_hostname = get_secret("mister-market-project", "db_hostname", "1")                  #for local dev
db_port = "3306"                                                                       #for local dev
db_ssl_ca_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/server-ca.pem'     #for local dev
db_ssl_cert_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-cert.pem' #for local dev
db_ssl_key_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-key.pem'   #for local dev

engine = db.create_engine(
  db.engine.url.URL.create(
    drivername=driver_name,
    username=db_user,
    password=db_password,
    database=db_name,
    #query=query_string,                  #for cloud function
    host=db_hostname,  # e.g. "127.0.0.1" #for local dev
    port=db_port,  # e.g. 3306            #for local dev
  ),
  pool_size=5,
  max_overflow=2,
  pool_timeout=30,
  pool_recycle=1800
  ,                                   #for local dev
  connect_args = {                    #for local dev
      'ssl_ca': db_ssl_ca_path ,      #for local dev
      'ssl_cert': db_ssl_cert_path,   #for local dev
      'ssl_key': db_ssl_key_path      #for local dev
      }                               #for loval dev
)

connection = engine.connect()
metadata = db.MetaData()

def get_price(ticker):
    #get raw price
    sql = "Select date,ticker,close From raw_data.company_stock Where ticker = " + "'" + ticker + "'" + " Order by date asc"
    #sql = "Select date,ticker,close From raw_data.company_stock Order by date asc"
    price_df = pd.read_sql(sql, connection)
    price_df = price_df[['date','ticker','close']]
    price_df['date'] = pd.to_datetime(price_df['date'], utc=True) #formatting
    price_df['close'] = price_df['close'].astype(float)

    # #get adjusted price
    sql = "Select date,ticker,adjusted_close From raw_data.company_adjusted_stock Where ticker = " + "'" + ticker + "'" + " Order by date asc"
    #sql = "Select date,ticker,adjusted_close From raw_data.company_adjusted_stock Order by date asc"
    adjusted_price_df = pd.read_sql(sql, connection)
    adjusted_price_df = adjusted_price_df[['date','ticker','adjusted_close']]
    adjusted_price_df['date'] = pd.to_datetime(adjusted_price_df['date'], utc=True) #formatting
    adjusted_price_df['adjusted_close'] = adjusted_price_df['adjusted_close'].astype(float)

    #merge dataframes
    price_df = pd.merge(price_df, adjusted_price_df, on=['date','ticker'], how='left')

    #prep output
    price_df = price_df.rename(columns={'close':'close_price', 'adjusted_close':'adjusted_close_price'})
    price_df = price_df[['ticker','date','close_price', 'adjusted_close_price']]

    return price_df

def get_shares_outstanding(ticker):
    sql = "Select date,fiscalDateEnding From " + "raw_data.company_earnings" + " Where ticker = " + "'" + ticker + "'" + " Order by date asc"
    #sql = "Select date,fiscalDateEnding From " + "raw_data.company_earnings Order by date asc"
    report_date_df = pd.read_sql(sql, connection)
    report_date_df = report_date_df[['date', 'fiscalDateEnding']]

    sql = "Select ticker,fiscalDateEnding,commonStockSharesOutstanding From " + "raw_data.company_balance_sheet" + " Where ticker = " + "'" + ticker + "'" + " Order by fiscalDateEnding asc"
    #sql = "Select ticker,fiscalDateEnding,commonStockSharesOutstanding From " + "raw_data.company_balance_sheet Order by fiscalDateEnding asc"
    bs_df = pd.read_sql(sql, connection)
    bs_df = bs_df[['ticker','fiscalDateEnding','commonStockSharesOutstanding']]
    bs_df[bs_df['commonStockSharesOutstanding'] == 'None'] = None

    sql = "Select date,ticker,split_coefficient From " + "raw_data.company_adjusted_stock" + " Where ticker = " + "'" + ticker + "'" + " Order by date asc"
    #sql = "Select date,ticker,split_coefficient From " + "raw_data.company_adjusted_stock Order by date asc"
    adjusted_stock_df = pd.read_sql(sql, connection)
    adjusted_stock_df  = adjusted_stock_df[['date','ticker','split_coefficient']]

    shares_df = pd.merge(report_date_df, bs_df, on='fiscalDateEnding', how='left') ### THERES an implicit assumption here that earnings release day = quarterlyReports day which is false
    shares_df = pd.merge(adjusted_stock_df, shares_df, on=['date','ticker'], how='left')

    #massage data types
    shares_df = shares_df.rename(columns={'commonStockSharesOutstanding':'last_reported_shares_outstanding','fiscalDateEnding':'last_reported_quarter'})

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
    shares_df = shares_df[['ticker','date','shares_outstanding']]
    shares_df['date'] = pd.to_datetime(shares_df['date'], utc=True) #formatting
    return shares_df


def get_dividends(ticker,shares_df):
    #get dividend data from adjusted price data
    sql = "Select ticker,date,dividend_amount From raw_data.company_adjusted_stock Where ticker = " + "'" + ticker + "'" + " Order by date asc"
    #sql = "Select ticker,date,dividend_amount From raw_data.company_adjusted_stock Order by date asc"
    dividend_df = pd.read_sql(sql, connection)
    dividend_df = dividend_df[['ticker','date','dividend_amount']]
    dividend_df['date'] = pd.to_datetime(dividend_df['date'], utc=True) #formatting
    dividend_df[dividend_df['dividend_amount'] == 'None'] = None

    #merge dividend and shares count
    dividend_df = pd.merge(dividend_df, shares_df, on=['ticker','date'], how='left')

    #create a dataframe for dates so when i compute dividend ttm is accurate
    start_date = dividend_df['date'].min()
    end_date = dividend_df['date'].max()
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True) #formatting

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
def get_earnings(ticker, shares_df):
    #get company earnings data
    sql = "Select date,ticker,reportedEPS From raw_data.company_earnings Where ticker = " + "'" + ticker + "'" + " Order by date asc"
    #sql = "Select date,ticker,reportedEPS From raw_data.company_earnings Order by date asc"
    earnings_df = pd.read_sql(sql, connection)
    earnings_df = earnings_df[['date', 'ticker', 'reportedEPS']]
    earnings_df['date'] = pd.to_datetime(earnings_df['date'], utc=True) #formatting

    #massage dataframe
    earnings_df = earnings_df.rename(columns={'reportedEPS':'non_gaap_eps'})
    earnings_df[earnings_df['non_gaap_eps'] == 'None'] = None
    earnings_df['non_gaap_eps'] = earnings_df['non_gaap_eps'].astype(float)

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
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True) #formatting

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

#I should add this to data functions
def get_meta_data(ticker):
    sql = "Select * From raw_data.company_overview Where ticker = " + "'" + ticker + "'"
    #sql = "Select * From raw_data.company_overview "
    meta_data_df = pd.read_sql(sql, connection)
    meta_data_df = meta_data_df[['ticker','Sector','Industry', 'Exchange','Currency','Name']]
    meta_data_df = meta_data_df.rename(columns={"Sector":"sector", "Industry":"industry","Currency":"currency","Name":"name", "Exchange":"exchange"}) #style thing much make lowe case some column names
    return meta_data_df


#run calcs by date
#run calcs by full history

#dont do this by ticker
def calc_company_measures(ticker):
    price_df = get_price(ticker) #date, ticker, close_price
    shares_df = get_shares_outstanding(ticker) #date, ticker, shares_outstanding
    dividends_df = get_dividends(ticker, shares_df) #date, ticker, dps_ttm, earnings_ttm
    earnings_df = get_earnings(ticker, shares_df) #date, ticker, non_gaap_eps_ttm, non_gaap_earnings_ttm
    meta_data_df = get_meta_data(ticker)

    #date, ticket, sector, industry, exchange, currency, country, price, ttm dividends, ttm earnings, shares, marketcap, payout ratio
    data_frames = [price_df, shares_df, dividends_df, earnings_df]
    measures_df = reduce(lambda  left,right: pd.merge(left,right,on=['date','ticker'], how='left'), data_frames)
    measures_df = pd.merge(measures_df, meta_data_df, on='ticker', how='left')

    #compute additional meaures
    measures_df['marketcap'] = measures_df['close_price'] * measures_df['shares_outstanding']
    measures_df['payout_ratio'] = measures_df['dps_ttm'] / measures_df['non_gaap_eps_ttm']
    measures_df['dividend_yield'] = measures_df['dps_ttm'] / measures_df['close_price']
    measures_df['earnings_yield'] = measures_df['non_gaap_eps_ttm'] / measures_df['close_price']

    #for ticker get company data and append it to company_stock table
    measures_df.to_sql('company_measures', engine, if_exists='append', index=False, chunksize=500)



def reset_company_measures()
