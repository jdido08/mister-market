#https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661
import yaml
import sqlalchemy
import pandas as pd
import fred

with open("config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

db_user = cfg["mysql"]["DB_USER"]
db_pass = cfg["mysql"]["DB_PASS"]
db_hostname = cfg["mysql"]["DB_HOSTNAME"]
db_port = cfg["mysql"]["DB_PORT"]
db_name = cfg["mysql"]["DB_NAME"]

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


sp500_price = fred.get_sp500_prices()
sp500_price.to_sql('sp500', engine, if_exists='replace', index=False, chunksize=500)
print(sp500_price)

# treasury_yields = fred.get_treasury_yields()
# treasury_yields.to_sql('treasury_yields', engine, if_exists='replace', index=False, chunksize=500)
#
# tips_yields = fred.get_tips_yields()
# tips_yields.to_sql('tips_yields', engine, if_exists='replace', index=False, chunksize=500)

engine.dispose()

# table_name = 'sp500'
# table_df = pd.read_sql_table(
#     table_name,
#     con=engine
# )
#
# print(table_df)
