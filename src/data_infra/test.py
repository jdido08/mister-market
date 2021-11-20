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
# df = pd.read_sql_table('company_measures', engine) #read in current sp500_constituents table
# df.to_csv("test.csv")
str_time = datetime.strftime(date_time, '%Y-%m-%d %H:%M:%S,%f')

# save data to database
try:
    company_stock_df = pd.read_csv('data\_test_company_stock.csv')
    company_stock_df.to_sql('company_stock', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_stock updated')
    print("SUCCESS: company_stock updated")
except Exception as e:
    logging.error("Can't update company_stock -- Error: ", e)
    print("ERROR: Can't update company_stock")

try:
    company_adjusted_stock_df = pd.read_csv('data\_test_company_adjusted_stock.csv')
    company_adjusted_stock_df.to_sql('company_adjusted_stock', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_adjusted_stock updated')
    print("SUCCESS: company_adjusted_stock updated")
except Exception as e:
    logging.error("Can't update company_adjusted_stock -- Error: ", e)
    print("ERROR: Can't update company_adjusted_stock")

try:
    company_earnings_df = pd.read_csv('data\_test_company_earnings.csv')
    company_earnings_df.to_sql('company_earnings', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_earnings updated')
    print("SUCCESS: company_earnings updated")
except Exception as e:
    logging.error("Can't update company_earnings -- Error: ", e)
    print("ERROR: Can't update company_earnings")

try:
    company_balance_sheet_df = pd.read_csv('data\_test_company_balance_sheet.csv')
    company_balance_sheet_df.to_sql('company_balance_sheet', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_balance_sheet updated')
    print("SUCCESS: company_balance_sheet updated")
except Exception as e:
    logging.error("Can't update company_balance_sheet -- Error: ", e)
    print("ERROR: Can't update company_balance_sheet")

try:
    company_overview_df = pd.read_csv('data\_test_company_overview.csv')
    company_overview_df.to_sql('company_overview', engine, if_exists='replace', index=False, chunksize=500)
    logging.info('SUCCESS: company_overview updated')
    print("SUCCESS: company_overview updated")
except Exception as e:
    logging.error("Can't update company_overview -- Error: ", e)
    print("ERROR: Can't update company_overview")


# try:
#     company_income_statement_df = pd.concat(company_income_statement_frames, axis=0, ignore_index=True)
#     company_income_statement_df.to_sql('company_income_statement', engine, if_exists='replace', index=False, chunksize=500)
#     logging.info('SUCCESS: company_income_statement updated')
#     print("SUCCESS: company_income_statement updated")
# except Exception as e:
#     logging.error("Can't update company_income_statement -- Error: ", e)
#     print("ERROR: Can't update company_income_statement")
#
# try:
#     company_cash_flow_statement_df = pd.concat(company_cash_flow_statement_frames, axis=0, ignore_index=True)
#     company_cash_flow_statement_df.to_sql('company_cash_flow_statement', engine, if_exists='replace', index=False, chunksize=500)
#     logging.info('SUCCESS: company_cash_flow_statement updated')
#     print("SUCCESS: company_cash_flow_statement updated")
# except Exception as e:
#     logging.error("Can't update company_cash_flow_statement -- Error: ", e)
#     print("ERROR: Can't update company_cash_flow_statement")
