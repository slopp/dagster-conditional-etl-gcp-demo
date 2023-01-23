import os

import pandas as pd
from dagster._config.structured_config import Resource, StructuredConfigIOManager
from google.oauth2 import service_account

class FileReader(Resource):
    def read(self, file) -> pd.DataFrame:
        return pd.read_csv(file)


class DirectoryLister(Resource):
    def list(self, dir):
        return os.listdir(dir)


class GCPAuth(Resource):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str
    


class GCPFileReader(Resource):
    auth: GCPAuth
    def read(self, file) -> pd.DataFrame:
        token = self.auth.dict(include={'type', 'project_id', 'private_key_id','private_key','client_email','client_id','auth_uri','token_uri','auth_provider_x509_cert_url','client_x509_cert_url'})
        credentials = service_account.Credentials.from_service_account_info(token, scopes=['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/devstorage.read_write'])
        return pd.read_csv(f"gcs://hooli-demo/{file}", storage_options = {'token': credentials})

class GCPFileWriter(StructuredConfigIOManager):
    auth: GCPAuth
    def handle_output(self, context, obj) -> None:
        file = f"{context.run_id}-{context.asset_key[0]}.csv"
        token = self.auth.dict(include={'type', 'project_id', 'private_key_id','private_key','client_email','client_id','auth_uri','token_uri','auth_provider_x509_cert_url','client_x509_cert_url'})
        credentials = service_account.Credentials.from_service_account_info(token, scopes=['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/devstorage.read_write'])
        obj.to_csv(f"gcs://hooli-demo/{file}", storage_options = {'token': credentials})
        return 
    
    def load_input(self, context):
        pass