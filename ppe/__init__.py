from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy as sqlalchemy #to save to db

#link
# https://github.com/CoreyMSchafer/code_snippets/tree/master/Python/Flask_Blog

app = Flask(__name__)
# app.config['SECRET_KEY'] = '???' #not sure I need that
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db' #need to come back to this


# Google Cloud SQL (change this accordingly)
import os #to get file path
from google.cloud import secretmanager # Import the Secret Manager client library.
import google_crc32c

def get_secret(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    #for local dev -- set google app credentials
    google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-6e485429eb5e.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path

    #link: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
    #follow instruction here to run locally: https://cloud.google.com/docs/authentication/production#create-service-account-gcloud

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
    else:
        payload = response.payload.data.decode("UTF-8")
        return payload


driver_name = 'mysql+pymysql'
db_user = "root"
db_name = "raw_data"
db_password = get_secret("mister-market-project", "db_password", "1")
db_hostname = get_secret("mister-market-project", "db_hostname", "1")                  #for local dev
db_port = "3306"                                                                       #for local dev
db_ssl_ca_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/server-ca.pem'     #for local dev
db_ssl_cert_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-cert.pem' #for local dev
db_ssl_key_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-key.pem'   #for local dev

db_url = sqlalchemy.engine.url.URL.create(
  drivername=driver_name,
  username=db_user,
  password=db_password,
  database=db_name,
  host=db_hostname,  # e.g. "127.0.0.1" #for local dev
  port=db_port,  # e.g. 3306            #for local dev
)

print(db_url)


# engine = db.create_engine(
#   db.engine.url.URL.create(
#     drivername=driver_name,
#     username=db_user,
#     password=db_password,
#     database=db_name,
#     #query=query_string,                  #for cloud function
#     host=db_hostname,  # e.g. "127.0.0.1" #for local dev
#     port=db_port,  # e.g. 3306            #for local dev
#   ),
#   pool_size=5,
#   max_overflow=2,
#   pool_timeout=30,
#   pool_recycle=1800
#   ,                                   #for local dev
#   connect_args = {                    #for local dev
#       'ssl_ca': db_ssl_ca_path ,      #for local dev
#       'ssl_cert': db_ssl_cert_path,   #for local dev
#       'ssl_key': db_ssl_key_path      #for local dev
#       }                               #for loval dev
# )

# configuration
app.config["SECRET_KEY"] = "yoursecretkey"
app.config["SQLALCHEMY_DATABASE_URI"]= db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS" ]= True
db = SQLAlchemy(app)

from ppe import routes
