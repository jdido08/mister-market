import yaml
import sqlalchemy
import pandas as pd


### FUNCTIONS ###

def get_sp_500_constituents_today():
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

    return df

def get_sp500_constituents():
    sql = "SELECT * FROM " + table_id + " Order by date  asc"
    sp_500_constituents_df = read_db(sql) #get existing sp_500_constituents

    #get dates from end of existing sp_500_constituents dataframe and today
    start_date = sp_500_constituents_df['date'].max() + datetime.timedelta(days=1) #find first date which there is no data for
    end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=start_date.tzinfo)
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')

    #get sp_500_constituents for today
    sp_500_constituents_today_df = get_sp_500_constituents_today()

    #append sp_500_constituents dataframe with constituent from today
    sp_500_constituents_today_df = pd.merge(date_df, sp_500_constituents_today_df, on='date', how='left')
    sp_500_constituents_df = pd.concat([sp_500_constituents_df, sp_500_constituents_today_df], ignore_index=True)

    sp_500_constituents_df = sp_500_constituents_df.sort_values(by='date') #order by dates
    sp_500_constituents_df = sp_500_constituents_df.drop_duplicates(subset=['date']) #drop any duplicates dates'; this unccessary now
    sp_500_constituents_df = sp_500_constituents_df.fillna(method='ffill') #fill forward for any misisng dataframes

    #upload new sp_500_constituents list
    write_to_db(sp_500_constituents_df, "raw_data.sp_500_constituents", "replace")

    print("sp_500_constituents updated!")



### SCRIPT ####

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


engine.dispose()

# table_name = 'sp500'
# table_df = pd.read_sql_table(
#     table_name,
#     con=engine
# )
#
# print(table_df)
