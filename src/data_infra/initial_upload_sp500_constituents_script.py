import yaml
import sqlalchemy
import pandas as pd
import datetime


### FUNCTIONS ####

def get_sp_500_constituents_historical():
    #data at https://github.com/fja05680/sp500
    start_date = datetime.datetime(1996, 1, 2).strftime("%Y-%m-%d") #start date from https://github.com/fja05680/sp500
    end_date = datetime.datetime(2021, 9, 24).strftime("%Y-%m-%d") #this potentially needs to be updated if the underlying csv updates
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')


    df = pd.read_csv('data\sp500_constituents_history_through_9_24_2021.csv')
    df['date'] = pd.to_datetime(df['date'],  format='%Y-%m-%d')

    df = pd.merge(date_df, df, on='date', how='left')
    df = df.sort_values(by='date')
    df = df.fillna(method='ffill')

    print(df)

    # #write historicals to big query
    # write_to_db(df, "raw_data.sp_500_constituents", "replace")


get_sp_500_constituents_historical()



### SCRIPT ####

# with open("config.yml", "r") as ymlfile:
#     cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
#
# db_user = cfg["mysql"]["DB_USER"]
# db_pass = cfg["mysql"]["DB_PASS"]
# db_hostname = cfg["mysql"]["DB_HOSTNAME"]
# db_port = cfg["mysql"]["DB_PORT"]
# db_name = cfg["mysql"]["RAW_DATA_DB_NAME"]
#
# #https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661
# engine = sqlalchemy.create_engine(
#      sqlalchemy.engine.url.URL.create(
#         drivername="mysql+pymysql",
#         username=db_user,  # e.g. "my-database-user"
#         password=db_pass,  # e.g. "my-database-password"
#         host=db_hostname,  # e.g. "127.0.0.1"
#         port=db_port,  # e.g. 3306
#         database=db_name,  # e.g. "my-database-name"
#     ),
#     connect_args = {
#         'ssl_ca': 'ssl/server-ca.pem',
#         'ssl_cert': 'ssl/client-cert.pem',
#         'ssl_key': 'ssl/client-key.pem'
#     }
# )
#
#
# engine.dispose()
