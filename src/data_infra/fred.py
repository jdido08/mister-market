#packages imported
import pandas_datareader as web
import pandas as pd
import datetime
import yaml

with open("config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

#get sp500 price data from FRED #https://fred.stlouisfed.org/series/SP500
def get_sp500_prices():
    sp500_fred_start_date = datetime.datetime(2010, 9, 13).strftime("%Y-%m-%d") #start date on FRED wesbite; I have a csv of historical data if I ever need it
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df= web.DataReader(['SP500'], 'fred', sp500_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={"DATE":"date", "SP500":"sp500_close"})
    df['sp500_close'] = pd.to_numeric(df['sp500_close'], errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d')

    return df


def get_treasury_yields():
    treasury_fred_start_date = datetime.datetime(1962, 1, 2).strftime("%Y-%m-%d") #start date on FRED wesbite
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
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d')

    return df

def get_tips_yields():
    tips_fred_start_date = datetime.datetime(2003, 1, 2).strftime("%Y-%m-%d") #start date on FRED wesbite
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df = web.DataReader(['DFII5','DFII7','DFII10','DFII20','DFII30'], 'fred',tips_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={
        "DATE":"date",
        "DFII5":"_5y",
        "DFII7":"_7y",
        "DFII10":"_10y",
        "DFII20":"_20y",
        "DFII30":"_30y"})
    df = df.sort_values(by='date')
    column_names = ['_5y','_7y','_10y','_20y','_30y']
    df[column_names] = df[column_names].apply(pd.to_numeric, errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d')
    return df
