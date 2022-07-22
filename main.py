

import pandas_datareader as web # to get fred data
import pandas as pd
import datetime
import yaml #to open config file
import sqlalchemy #to save to db
import os #to get file path
import logging #to log


### FUNCTIONS ###
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

def test_db_writing(request):
    ### SCRIPT ####
    ### THIS SCRIPT IS MEANT TO RUN ONCE A DAY ###
    # logging.basicConfig(filename= os.path.dirname(os.path.abspath(__file__)) + '/ingest_logs.log',
    #     level=logging.DEBUG,
    #     filemode='w',
    #     format='%(asctime)s - %(levelname)s - %(message)s',
    #     datefmt='%m/%d/%Y %I:%M:%S %p')
    #
    #
    # config_file_path = os.path.dirname(os.path.abspath(__file__)) + "/config.yml"
    #
    # with open(config_file_path, "r") as ymlfile:
    #     cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)






    # db_user = cfg["mysql"]["DB_USER"]
    # db_pass = cfg["mysql"]["DB_PASS"]
    # db_hostname = cfg["mysql"]["DB_HOSTNAME"]
    # db_port = cfg["mysql"]["DB_PORT"]
    # db_name = cfg["mysql"]["RAW_DATA_DB_NAME"]
    connection_name = "mister-market-project:us-central1:mister-market-db"
    db_user = "root"
    db_password = "B4Nqi68k19megKGd"
    db_hostname = "34.133.165.86"
    db_port = "3306"
    db_name = "raw_data"

    # If your database is MySQL, uncomment the following two lines:
    driver_name = 'mysql+pymysql'
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

    # db_ssl_ca = cfg["mysql"]["SERVER-CA.PEM"]
    # db_ssl_cert = cfg["mysql"]["CLIENT-CERT.PEM"]
    # db_ssl_key = cfg["mysql"]["CLIENT-KEY.PEM"]

    # db_ssl_ca_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/server-ca.pem'
    # db_ssl_cert_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-cert.pem'
    # db_ssl_key_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-key.pem'

    #for local dev -- set google app credentials
    # google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-6e485429eb5e.json"
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path

    #https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661
    # engine = sqlalchemy.create_engine(
    #      sqlalchemy.engine.url.URL.create(
    #         drivername="mysql+pymysql",
    #         username=db_user,  # e.g. "my-database-user"
    #         password=db_pass,  # e.g. "my-database-password"
    #         host=db_hostname,  # e.g. "127.0.0.1"
    #         port=db_port,  # e.g. 3306
    #         database=db_name,  # e.g. "my-database-name"
    #     ),
    #     # connect_args = {
    #     #     # 'ssl_ca': db_ssl_ca,
    #     #     # 'ssl_cert': db_ssl_cert,
    #     #     # 'ssl_key': db_ssl_key
    #     #     'ssl_ca': db_ssl_ca_path ,
    #     #     'ssl_cert': db_ssl_cert_path,
    #     #     'ssl_key': db_ssl_key_path
    #     # }
    # )

    engine = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )




    # this is what you want to run daily
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


    # try:
    #     with db.connect() as conn:
    #         conn.execute(stmt)
    # except Exception as e:
    #     return 'Error: {}'.format(str(e))

    try:
        #print(sp500_constituents_df)
        sp500_constituents_df.to_sql('sp500_constituents', engine, if_exists='replace', index=False, chunksize=500)
        #logging.info('SUCCESS: sp500_constituents updated')
        print("SUCCESS: sp500_constituents updated")
    except Exception as e:
        #logging.error("Can't update sp500_constituents -- Error: ", e)
        print("Can't update sp500_constituents -- Error: ", e)





# 
# request = ""
# test_db_writing(request)










# This file contains all the code used in the codelab.
# import sqlalchemy

# Depending on which database you are using, you'll set some variables differently.
# In this code we are inserting only one field with one value.
# Feel free to change the insert statement as needed for your own table's requirements.

# Uncomment and set the following variables depending on your specific instance and database:
# connection_name = ""
# table_name = ""
# table_field = ""
# table_field_value = ""
# db_name = ""
# db_user = "root"
# db_password = "B4Nqi68k19megKGd"




# If your database is MySQL, uncomment the following two lines:
#driver_name = 'mysql+pymysql'
#query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

# If your database is PostgreSQL, uncomment the following two lines:
#driver_name = 'postgres+pg8000'
#query_string =  dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(connection_name)})

# If the type of your table_field value is a string, surround it with double quotes.

# def insert():
#     # request_json = request.get_json()
#     # stmt = sqlalchemy.text('insert into {} ({}) values ({})'.format(table_name, table_field, table_field_value))
#
#     db = sqlalchemy.create_engine(
#       sqlalchemy.engine.url.URL(
#         drivername='mysql+pymysql',
#         username=db_user,
#         password=db_password,
#         database=db_name,
#         query=query_string,
#       ),
#       pool_size=5,
#       max_overflow=2,
#       pool_timeout=30,
#       pool_recycle=1800
#     )
#     try:
#         with db.connect() as conn:
#             conn.execute(stmt)
#     except Exception as e:
#         return 'Error: {}'.format(str(e))
#     return 'ok'








# def get_sp500_constituents_today():
#     import pandas as pd
#     import datetime
#     data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S&P_500_companies')
#
#     # Get current S&P table and set header column
#     df = data[0].iloc[1:,[0]] #get certain columns in first table on wiki page
#     columns = ['ticker']
#     df.columns = columns
#
#     tickers_list = list(set(df['ticker'])) #remove any accidnetal duplicates
#     tickers_list = sorted(tickers_list)
#     tickers_str = ','.join(tickers_list)
#
#     #reformat
#     date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=datetime.timezone.utc)
#
#
#     df = pd.DataFrame(columns= {'date', 'tickers'}) #crate empty dataframe
#     df = df.append({'date':date, 'tickers':tickers_str}, ignore_index = True)
#     df['date'] = pd.to_datetime(df['date'], utc=True)
#
#     return df

#print(get_sp500_constituents_today())

























def get_secret(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    #for local dev -- set google app credentials
    # google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-6e485429eb5e.json"
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path

    #link: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
    #follow instruction here to run locally: https://cloud.google.com/docs/authentication/production#create-service-account-gcloud

    # project_id = "mister-market-project"
    # secret_id = "alphavantage_special_key"
    # version_id = "1"

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

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    #payload = response.payload.data.decode("UTF-8")
    # print("Plaintext: {}".format(payload))
