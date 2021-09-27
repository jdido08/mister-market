import yaml
import sqlalchemy
import pandas as pd
import datetime


### FUNCTIONS ###

def get_historical_sp500_constituents():
    #data at https://github.com/fja05680/sp500
    start_date = datetime.datetime(1996, 1, 2).strftime("%Y-%m-%d") #start date from https://github.com/fja05680/sp500
    end_date = datetime.datetime(2021, 9, 25).strftime("%Y-%m-%d") #this potentially needs to be updated if the underlying csv updates
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True) #formatting

    df = pd.read_csv('data\sp500_constituents_history_through_9_25_2021.csv')
    df['date'] = pd.to_datetime(df['date'], utc=True) #formatting

    df = pd.merge(date_df, df, on='date', how='left')
    df = df.sort_values(by='date')
    df = df.fillna(method='ffill')

    return df

def get_sp500_constituents_today():
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S&P_500_companies')

    # Get current S&P table and set header column
    df = data[0].iloc[1:,[0]] #get certain columns in first table on wiki page
    columns = ['ticker']
    df.columns = columns

    tickers_list = list(set(df['ticker'])) #remove any accidnetal duplicates
    tickers_list = sorted(tickers_list)
    tickers_str = ','.join(tickers_list)

    #reformat
    date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=datetime.timezone.utc)


    df = pd.DataFrame(columns= {'date', 'tickers'}) #crate empty dataframe
    df = df.append({'date':date, 'tickers':tickers_str}, ignore_index = True)
    df['date'] = pd.to_datetime(df['date'], utc=True)

    return df


### SCRIPT ####
### THIS SCRIPT IS MEANT TO RUN ONCE A DAY ###

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

#dont really ever run this
# def upload_historical_sp500_constituents():
#     try:
#         historical_sp500_constituents = get_historical_sp500_constituents()
#         historical_sp500_constituents.to_sql('sp500_constituents', engine, if_exists='replace', index=False, chunksize=500)
#     except:
#         print("ERROR: Can't update sp500_constituents")


def update_sp500_constituents():
    sp500_constituents_df = pd.read_sql_table('sp500_constituents', engine) #read in current sp500_constituents table
    sp500_constituents_df['date'] = pd.to_datetime(sp500_constituents_df['date'], utc=True)

    #get dates from end of existing sp500_constituents dataframe and today
    start_date = sp500_constituents_df['date'].max() + datetime.timedelta(days=1) #find first date which there is no data for
    end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=start_date.tzinfo)
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date') #list of dates from last date of current table and today


    #get sp_500_constituents for today
    sp500_constituents_today_df = get_sp500_constituents_today()


    #append sp500_constituents dataframe with constituent from today
    sp500_constituents_today_df = pd.merge(date_df, sp500_constituents_today_df, on='date', how='left')
    sp500_constituents_df = pd.concat([sp500_constituents_df, sp500_constituents_today_df], ignore_index=True)

    #format
    sp500_constituents_df = sp500_constituents_df.sort_values(by='date') #order by dates
    sp500_constituents_df = sp500_constituents_df.drop_duplicates(subset=['date']) #drop any duplicates dates'; this unccessary now
    sp500_constituents_df = sp500_constituents_df.fillna(method='ffill') #fill forward for any misisng dataframes

    try:
        sp500_constituents_df.to_sql('sp500_constituents', engine, if_exists='replace', index=False, chunksize=500)
        print("SUCCESS: sp500_constituents updated")
    except:
        print("ERROR: Can't update sp500_constituents")
