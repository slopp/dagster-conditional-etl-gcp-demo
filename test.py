from dagster_project.resources import GCPAuth, GCPFileReader, GCPFileWriter
import os

from dagster import build_output_context

gcp_auth = GCPAuth(
  type="service_account",
  project_id= os.getenv('GCP_PROJECT_ID'),
  private_key_id= os.getenv("GCP_SA_PRIVATE_KEY_ID"),
  private_key=os.getenv("GCP_SA_PRIVATE_KEY"),
  client_email=os.getenv("GCP_SA_CLIENT_EMAIL"),
  client_id=os.getenv("GCP_SA_CLIENT_ID"),
  auth_uri="https://accounts.google.com/o/oauth2/auth",
  token_uri="https://oauth2.googleapis.com/token",
  auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
  client_x509_cert_url=os.getenv("GCP_SA_CLIENT_CERT")
)

gcp_file_reader = GCPFileReader(auth=gcp_auth)
gcp_file_writer = GCPFileWriter(auth=gcp_auth)

parts = gcp_file_reader.read("parts.csv")

context = build_output_context(step_key="test", asset_key=["test", "asset"], run_id="test")

gcp_file_writer.handle_output(context, parts)