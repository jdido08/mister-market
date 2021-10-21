import yaml
import sqlalchemy
import pandas as pd
import datetime



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
        port=db_port,      # e.g. 3306
        database=db_name,  # e.g. "my-database-name"
    ),
    connect_args = {
        'ssl_ca': 'server-ca.pem',
        'ssl_cert': 'client-cert.pem',
        'ssl_key': 'client-key.pem'
    }
)


sp500_constituents_df = pd.read_sql_table('sp500_constituents', engine) #read in current sp500_constituents table
sp500_constituents_df['date'] = pd.to_datetime(sp500_constituents_df['date'], utc=True)
print(sp500_constituents_df)


#test
