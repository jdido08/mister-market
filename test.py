import argparse
import google_crc32c
import os

#for local dev -- set google app credentials
# google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-6e485429eb5e.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path

#link: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
#follow instruction here to run locally: https://cloud.google.com/docs/authentication/production#create-service-account-gcloud

def access_secret_version(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

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
    payload = response.payload.data.decode("UTF-8")
    print("Plaintext: {}".format(payload))


#access_secret_version("mister-market-project", "alphavantage_special_key", "1")


#
# # Import the Secret Manager client library.
# from google.cloud import secretmanager
#
# # GCP project in which to store secrets in Secret Manager.
# project_id = "mister-market-project"
#
# # ID of the secret to create.
# secret_id = "alphavantage_special_key"
#
# # Create the Secret Manager client.
# client = secretmanager.SecretManagerServiceClient()
#
# # Build the parent name from the project.
# parent = f"projects/{project_id}"
#
# # Create the parent secret.
# secret = client.create_secret(
#     request={
#         "parent": parent,
#         "secret_id": secret_id,
#         "secret": {"replication": {"automatic": {}}},
#     }
# )
#
# # Add the secret version.
# version = client.add_secret_version(
#     request={"parent": secret.name, "payload": {"data": b"hello world!"}}
# )
#
# # Access the secret version.
# response = client.access_secret_version(request={"name": version.name})
#
# # Print the secret payload.
# #
# # WARNING: Do not print the secret in a production environment - this
# # snippet is showing how to access the secret material.
# payload = response.payload.data.decode("UTF-8")
# print("Plaintext: {}".format(payload))
