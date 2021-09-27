import pandas_datareader as web
import pandas as pd
import datetime
import yaml
import sqlalchemy

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


def get_treasury_yields():
    treasury_fred_start_date = datetime.datetime(2000, 1, 1).strftime("%Y-%m-%d") #rates go back to 1962 but just starting with 2000
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
    df['date'] =  pd.to_datetime(df['date'], utc=True)

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
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d', utc=True)
    return df


#### UPLOAD SCRIPTs ####

with open("config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

db_user = cfg["mysql"]["DB_USER"]
db_pass = cfg["mysql"]["DB_PASS"]
db_hostname = cfg["mysql"]["DB_HOSTNAME"]
db_port = cfg["mysql"]["DB_PORT"]
db_name = cfg["mysql"]["RAW_DATA_DB_NAME"]

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
        'ssl_ca': 'ssl/server-ca.pem',
        'ssl_cert': 'ssl/client-cert.pem',
        'ssl_key': 'ssl/client-key.pem'
    }
)


def update_sp500_prices():
    try:
        sp500_price = get_sp500_prices()
        sp500_price.to_sql('sp500_prices', engine, if_exists='replace', index=False, chunksize=500)
        print("SUCCESS: sp500_prices updated")
    except:
        print("ERROR: Can't update sp500_prices")


def update_treasury_yields():
    try:
        treasury_yields = get_treasury_yields()
        treasury_yields.to_sql('treasury_yields', engine, if_exists='replace', index=False, chunksize=500)
        print("SUCCESS: treasury_yields updated")
    except:
        print("ERROR: Can't update treasury_yields")


def update_tips_yields():
    try:
        tips_yields = get_tips_yields()
        tips_yields.to_sql('tips_yields', engine, if_exists='replace', index=False, chunksize=500)
        print("SUCCESS: tips_yields updated")
    except:
        print("ERROR: Can't update tips_yields")


#engine.dispose()

# table_name = 'sp500'
# table_df = pd.read_sql_table(
#     table_name,
#     con=engine
# )
#
# print(table_df)
