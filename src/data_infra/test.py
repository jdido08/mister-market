import pandas as pd
import yaml
import sqlalchemy #to save to db
import os #to get file path
import logging #to log



#### UPDATE COMPANY DATA SCRIPT ####

logging.basicConfig(filename= os.path.dirname(os.path.abspath(__file__)) + '/ingest_logs.log',
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


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



#company, date table

#for today our universe of companies is the sp500
df = pd.read_sql_table('company_measures', engine) #read in current sp500_constituents table
df.to_csv("test.csv")
