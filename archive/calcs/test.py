import pandas as pd
import numpy as np
import yaml #to open config file
import sqlalchemy #to save to db
import os #to get file path
import scipy.interpolate #for interpolate function
import datetime


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


#calc risk-free rates (rf_rates)
rf_rates_df = pd.read_sql_table('treasury_yields', engine) #read in current treasury yields  table
rf_rates_df = rf_rates_df.set_index('date')
rf_rates_df = rf_rates_df.apply(lambda x : x / 100) #convert to decimal form
rf_rates_df = rf_rates_df.apply(lambda x : np.power(1+(x/2),2) - 1) #convert to annual effective
rf_rates_s = rf_rates_df.apply(lambda x : scipy.interpolate.interp1d([1,2,3,5,7,10,20,30], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
rf_rates_s = rf_rates_s.apply(lambda x : bootstrap(x))

#calc real rates (real_rates)
real_rates_df = pd.read_sql_table('tips_yields', engine) #read in current treasury yields  table
real_rates_df = real_rates_df.set_index('date')
real_rates_df = real_rates_df.apply(lambda x : x / 100) #convert to decimal form
real_rates_df = real_rates_df.apply(lambda x : np.power(1+(x/2),2) - 1) #convert to annual effective
real_rates_s = real_rates_df.apply(lambda x : scipy.interpolate.interp1d([5,7,10,20,30], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
real_rates_s = real_rates_s.apply(lambda x : bootstrap(x))

#calc risk preium rates (rp_rates) -- #set flat curve
rp_rates_df = pd.read_sql_table('treasury_yields', engine) #read in current treasury yields  table
rp_rates_df['risk_premium_x'] = .001 #set flat rp curve
rp_rates_df['risk_premium_y'] = .001 #set flat rp curve
rp_rates_df = rp_rates_df.set_index('date')
rp_rates_df = rp_rates_df[['risk_premium_x', 'risk_premium_y']]
rp_rates_s = rp_rates_df.apply(lambda x : scipy.interpolate.interp1d([5,10], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating

rates_df = pd.concat([rf_rates_s,real_rates_s,rp_rates_s], axis=1)
rates_df.columns =['risk_free_rates','real_rates','risk_premium_rates']
rates_df = rates_df.reset_index()
rates_df['date'] = pd.to_datetime(rates_df['date'] , format="%Y-%m-%d", utc=True) #change format type
print(rates_df)

#calc market diviends, earnings

#find consittuens
# # ****** I STILL NEED TO FILTER OUT DUPLICATES *******
sp500_df = pd.read_sql_table('sp500_constituents', engine)
sp500_df = sp500_df.loc[sp500_df['date'] >= np.datetime64(cfg["startdate"]) ]#filter for only the dates you want; will delete later
sp500_df['date'] = pd.to_datetime(sp500_df['date'] , format="%Y-%m-%d", utc=True) #change format type
sp500_df['tickers'] = sp500_df['tickers'].str.split(',')
sp500_df = sp500_df.explode('tickers') #break out so each date, ticker is unique
sp500_df = sp500_df.rename(columns = {'tickers':'ticker'})
sp500_df['sp500'] = 'x'

#
company_df = pd.read_sql_table('company_measures', engine)
company_df['date'] = pd.to_datetime(company_df['date'] , format="%Y-%m-%d", utc=True) #change format type
company_df = pd.merge(sp500_df, company_df, on=['date','ticker'], how='inner')
#company_df.to_csv('company.csv')

sp500_price_df = pd.read_sql_table('sp500_prices', engine)
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



market_df = pd.merge(market_df, rates_df, on='date', how='inner')

print(market_df)
market_df['growth_rate'] = market_df.apply(lambda x : calc_growth_rate(x), axis=1) #convert to decimal form
print(market_df.columns)
print(market_df)
market_df.to_csv('market2.csv')

# m_df.to_csv('market.csv')
# print(m_df)
