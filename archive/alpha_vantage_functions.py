import requests
import pandas as pd
import csv
import io
import yaml

#alphavantage documentation: https://www.alphavantage.co/documentation/

with open("config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    alpha_vantage_api_key =  cfg["alphavantage"]["API_KEY"]

#core functions

#fundamental data
def get_alpha_vantage_fundamental_data(function, ticker, json_object):
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
def get_alpha_vantage_stock_time_series_data(function, ticker, outputsize):
    base_url = 'https://www.alphavantage.co/query?'
    params = {'function': function,
             'symbol': ticker,
             "datatype": 'csv',
             'outputsize': outputsize, #output size options: full, compact
             'apikey': alpha_vantage_api_key}
    response = requests.get(base_url, params=params)
    df = pd.read_csv(io.StringIO(response.text))
    df['ticker'] = ticker #create column with ticker
    df['timestamp'] = pd.to_datetime(df['timestamp'] , format="%Y-%m-%d", utc=True) #change format type #not right type;
    return df

# secondary functions -- for convience

def get_av_daily_stock(ticker, outputsize):
    df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY', ticker, outputsize)
    df = df.rename(columns={'timestamp':'date'}) #standardize naming convention
    #formating data
    return df

def get_av_daily_adjusted_stock(ticker, outputsize):
    df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY_ADJUSTED', ticker, outputsize)
    df = df.rename(columns={'timestamp':'date'}) #standardize naming convention
    return df

def get_av_quarterly_earnings(ticker):
    df = get_alpha_vantage_fundamental_data('EARNINGS', ticker, 'quarterlyEarnings')
    df = df.rename(columns = {'reportedDate':'date'}) #standardize naming convention
    df['date'] = pd.to_datetime(df['date'] , format="%Y-%m-%d", utc=True) #change format time
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    return df

def get_av_quarterly_income_statements(ticker):
    df = get_alpha_vantage_fundamental_data('INCOME_STATEMENT', ticker, 'quarterlyReports')
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    return df

def get_av_quarterly_balance_sheets(ticker):
    df = get_alpha_vantage_fundamental_data('BALANCE_SHEET', ticker, 'quarterlyReports')
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df.loc[df['commonStockSharesOutstanding'] == 'None', 'commonStockSharesOutstanding'] = None
    return df

def get_av_quarterly_cash_flow_statements(ticker):
    df = get_alpha_vantage_fundamental_data('CASH_FLOW', ticker, 'quarterlyReports')
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    return df

def get_av_company_overview(ticker):
    df = get_alpha_vantage_fundamental_data('OVERVIEW', ticker, None)
    df = df.rename(columns = {'52WeekHigh':'_52WeekHigh',
                              '52WeekLow':'_52WeekLow',
                              '50DayMovingAverage':'_50DayMovingAverage',
                              '200DayMovingAverage':'_200DayMovingAverage'}) #cant start field name with number

    return df



#test code
df = get_av_adjusted_stock('ABC','full')
print(df)
#df =  get_av_company_overview('XLE')
#df.to_csv('xle_stock_test.csv')
#print(df)

# df = get_av_quarterly_earnings('IBM')
# print(df)
#
# df = get_av_daily_stock('SPX','full')
# print(df)
#df.to_csv('SP500-55_test.csv')


#testing code
# df1 =  get_av_quarterly_balance_sheets('IBM')
# df2 =  get_av_quarterly_income_statements('IBM')
# df3 =  get_av_quarterly_earnings('IBM')
#
# df = pd.merge(df1, df2, on='fiscalDateEnding', how='outer')
# df = pd.merge(df, df3, on='fiscalDateEnding', how='outer')
#
# df = df[['ticker','fiscalDateEnding','netIncomeApplicableToCommonShares','operatingIncome','commonStockSharesOutstanding','commonStock','reportedEPS']]
#
# df.to_csv('ibm_fundamentals_test.csv')
