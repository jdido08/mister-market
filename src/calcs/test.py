import pandas as pd
import numpy as np
import yaml #to open config file
import sqlalchemy #to save to db
import os #to get file path
import scipy.interpolate #for interpolate function


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

def bootstrap(ytms): #this function is setup for yearly calcs
    zeros = [] #create empty zeros list
    zeros.append(ytms(1))
    for i in range(1, 30): #30 is the end year for now
        discounted_cfs = []
        face = 100
        coupon = (ytms(i+1))*face #coupon rate is equal to ytm because bond priced at par
        for j in range(1, i):
            dcf = coupon/np.power(1+zeros[j],j+1)
            discounted_cfs.append(dcf) #this is redundant to recalc previous DCFs each time but going to keep it for now
        z = np.power((face+coupon) / (face - sum(discounted_cfs)),1/(i+1)) - 1
        zeros.append(z)
    years = list(range(1,30 + 1))
    zeros = scipy.interpolate.interp1d(years, zeros, bounds_error=False, fill_value="extrapolate")
    return zeros #return interpole object


rfr_df = pd.read_sql_table('treasury_yields', engine) #read in current treasury yields  table
rfr_df = rfr_df.set_index('date')
rfr_df = rfr_df.apply(lambda x : x / 100) #convert to decimal form
rfr_df = rfr_df.apply(lambda x : np.power(1+(x/2),2) - 1) #convert to annual effective
rfr_s = rfr_df.apply(lambda x : scipy.interpolate.interp1d([1,2,3,5,7,10,20,30], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
rfr_s = rfr_s.apply(lambda x : bootstrap(x))

r_df = pd.read_sql_table('tips_yields', engine) #read in current treasury yields  table
r_df = r_df.set_index('date')
r_df = r_df.apply(lambda x : x / 100) #convert to decimal form
r_df = r_df.apply(lambda x : np.power(1+(x/2),2) - 1) #convert to annual effective
r_s = r_df.apply(lambda x : scipy.interpolate.interp1d([5,7,10,20,30], x, bounds_error=False, fill_value="extrapolate"), axis=1) #get ytm curve by interpolating
r_s = r_s.apply(lambda x : bootstrap(x))


#add risk premium later
df = pd.concat([rfr_s,r_s], axis=1)
print(df)

#calc earnings 
