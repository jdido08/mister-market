import sqlalchemy as db #to save to db
import pandas as pd
from google.cloud import secretmanager # Import the Secret Manager client library.
import google_crc32c
import os #to get file path
from functools import reduce
import datetime
import numpy as np
import scipy.interpolate #for interpolate function

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


################################################################################
######################## COMPANY CALC FUNCTIONS ################################
################################################################################

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

    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:
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
        #print(measures_df.dtypes)

        #set status of query
        update_status_query = db.update(company_data_status).values(calc_company_measures_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        #logging.info('SUCCESS: %s stock data updated', ticker)
        print('SUCCESS: ', ticker, ' calc company measures updated!')

    except Exception as e:
        #set status of query
        update_status_query = db.update(company_data_status).values(calc_company_measures_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        #logging.error('ERROR: Cant update company stock data -- Error: ', e)
        print('ERROR: Cant update ', ticker, ' calc company measures! DETAILS: ', e)

    #update status
    connection.execute(update_status_query)
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(calc_company_measures_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)



################################################################################
######################## MARKET CALC FUNCTIONS #################################
################################################################################


def bootstrap(ytms): #this function is setup for yearly calcs
    zeros = [] #create empty zeros list
    zeros.append(ytms(1))
    for i in range(1, 30): #30 is the end year for now
        discounted_cfs = []
        face = 100
        coupon = (ytms(i+1))*face #coupon rate is equal to ytm because bond priced at par
        for j in range(1, i):
            dcf = coupon/np.power(1+zeros[j],j+1)
            discounted_cfs.append(dcf) #this is redundant to recalc previous DCFs each time but going to keep it for now
        z = np.power((face+coupon) / (face - sum(discounted_cfs)),1/(i+1)) - 1
        zeros.append(z)
    years = list(range(1,30 + 1))
    zeros = scipy.interpolate.interp1d(years, zeros, bounds_error=False, fill_value="extrapolate")
    return zeros #return interpole object

def calc_growth_rate(market):

    long_term_coefficient = 0

    coefficients = [] #create coefficents list
    for i in range(1,1000): #market does not see further out than 30 years

        if(i < 30):
            coefficient = market['dividends_ttm'] / (np.power(1+market['risk_free_rates'](i),i) * np.power(1+market['risk_premium_rates'](i),i))
            coefficients.append(coefficient)
        elif(i == 30):
            coefficient = market['dividends_ttm'] / (np.power(1+market['risk_free_rates'](i),i) * np.power(1+market['risk_premium_rates'](i),i))
            long_term_coefficient = long_term_coefficient + coefficient
        else:
            coefficient = (market['dividends_ttm'] *  np.power(1+market['risk_free_rates'](30), i - 30) ) / (np.power(1+market['risk_free_rates'](30),i) * np.power(1+market['risk_premium_rates'](30),i))
            long_term_coefficient = long_term_coefficient + coefficient

    coefficients.append(long_term_coefficient)
    coefficients = coefficients[::-1] #reverse order of list
    #coefficients.insert(0,(-1*market['sp500_close'])) #insert negative price at beginning of list
    coefficients.append(-1*market['sp500_close'])
    #coefficients = coefficients[::-1] #reverse order of list
    #p = np.poly1d(coefficients])
    #print(coefficients)

    try:
        roots = np.roots(coefficients) #solve for roots
        roots = roots[np.isreal(roots)] #find only real roots i.e. no imaginery component
        growth_rate = roots[roots>0]  #find only real positive roots
        #growth_rate = (growth_rate[0].real - 1) #get growth

    except:
        growth_rate = None
        print("Issue calcing growht rate on: ", market['date'])
    return growth_rate




# #calc risk-free rates (rf_rates)
# rf_rates_df = pd.read_sql_table('treasury_yields', engine) #read in current treasury yields  table
# rf_rates_df = rf_rates_df.set_index('date')
# rf_rates_df = rf_rates_df.apply(lambda x : x / 100) #convert to decimal form
# rf_rates_df = rf_rates_df.apply(lambda x : np.power(1+(x/2),2) - 1) #convert to annual effective
# rf_rates_s = rf_rates_df.apply(lambda x : scipy.interpolate.interp1d([1,2,3,5,7,10,20,30], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
# rf_rates_s = rf_rates_s.apply(lambda x : bootstrap(x))
# #print(rf_rates_s)
#
#
# #calc real rates (real_rates)
# real_rates_df = pd.read_sql_table('tips_yields', engine) #read in current treasury yields  table
# real_rates_df = real_rates_df.set_index('date')
# real_rates_df = real_rates_df.apply(lambda x : x / 100) #convert to decimal form
# real_rates_df = real_rates_df.apply(lambda x : np.power(1+(x/2),2) - 1) #convert to annual effective
# real_rates_s = real_rates_df.apply(lambda x : scipy.interpolate.interp1d([5,7,10,20,30], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
# real_rates_s = real_rates_s.apply(lambda x : bootstrap(x))
# #print(real_rates_s)
#
# #calc inflation rates here
#
#
# #calc risk preium rates (rp_rates) -- #set flat curve
# rp_rates_df = pd.read_sql_table('treasury_yields', engine) #read in current treasury yields  table
# rp_rates_df['risk_premium_x'] = .000 #set flat rp curve
# rp_rates_df['risk_premium_y'] = .000 #set flat rp curve
# rp_rates_df = rp_rates_df.set_index('date')
# rp_rates_df = rp_rates_df[['risk_premium_x', 'risk_premium_y']]
# rp_rates_s = rp_rates_df.apply(lambda x : scipy.interpolate.interp1d([5,10], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
# #print(rp_rates_s)
#
#
# rates_df = pd.concat([rf_rates_s,real_rates_s,rp_rates_s], axis=1)
# rates_df.columns =['risk_free_rates','real_rates','risk_premium_rates']
# rates_df = rates_df.reset_index()
# rates_df['date'] = pd.to_datetime(rates_df['date'] , format="%Y-%m-%d", utc=True) #change format type
# #print(rates_df)
#
#
# #company - market,
#
# #market - price,
#
# #calc market diviends, earnings
#
#find consittuens
#****** I STILL NEED TO FILTER OUT DUPLICATES *******
sp500_df = pd.read_sql_table('sp500_constituents', engine)
sp500_df = sp500_df.loc[sp500_df['date'] >= np.datetime64("2021-01-01") ]#filter for only the dates you want; will delete later; for testing
sp500_df['date'] = pd.to_datetime(sp500_df['date'] , format="%Y-%m-%d", utc=True) #change format type
sp500_df['tickers'] = sp500_df['tickers'].str.split(',')
sp500_df = sp500_df.explode('tickers') #break out so each date, ticker is unique
sp500_df = sp500_df.rename(columns = {'tickers':'ticker'})
sp500_df = sp500_df.drop_duplicates(subset=['date','ticker']) #drop any duplicates dates, ticker combo'; probs unccesary but just in case
sp500_df['sp500'] = 'x' #mark as part of sp500
#print(sp500_df)



company_df = pd.read_sql_table('company_measures', engine)
#formating -- will need to delete later and save dtypes into sql table
company_df['marketcap'] = company_df['marketcap'].astype(float)
company_df['dividends_ttm'] = company_df['dividends_ttm'].astype(float)
company_df['non_gaap_earnings_ttm'] = company_df['non_gaap_earnings_ttm'].astype(float)
company_df['date'] = pd.to_datetime(company_df['date'] , format="%Y-%m-%d", utc=True) #change format type

company_df = pd.merge(sp500_df, company_df, on=['date','ticker'], how='inner')


#company_df.to_csv('company.csv')

sp500_price_df = pd.read_sql_table('sp500_prices', engine)
#formating -- will need to delete later and save dtypes into sql table
sp500_price_df['sp500_close'] = sp500_price_df['sp500_close'].astype(float)
sp500_price_df['date'] = pd.to_datetime(sp500_price_df['date'] , format="%Y-%m-%d", utc=True) #change format type


market_df = company_df.groupby(['date','sp500'])[['marketcap','dividends_ttm','non_gaap_earnings_ttm']].sum()
market_df = pd.merge(market_df, sp500_price_df, on=['date'], how='inner') #merge in sp500 price
market_df = market_df.loc[market_df['date'] >= '2020-01-01' ]#filter for only the dates you want; will delete later
market_df['date'] = pd.to_datetime(market_df['date'] , format="%Y-%m-%d", utc=True) #change format type
market_df['divisor'] = market_df['marketcap'] / market_df['sp500_close']
market_df['dividends_ttm'] = market_df['dividends_ttm'] / market_df['divisor']
market_df['non_gaap_earnings_ttm'] = market_df['non_gaap_earnings_ttm'] / market_df['divisor']
market_df['payout_ratio'] = market_df['dividends_ttm'] / market_df['non_gaap_earnings_ttm']
market_df = market_df.replace(np.nan, 0)
market_df.to_csv('market1.csv')
print(market_df)

market_df = pd.merge(market_df, rates_df, on='date', how='inner')

print(market_df)
market_df['growth_rate'] = market_df.apply(lambda x : calc_growth_rate(x), axis=1) #convert to decimal form
print(market_df.columns)
print(market_df)
market_df.to_csv('market2.csv')

# #
# # # m_df.to_csv('market.csv')
# # # print(m_df)
# #
