import logging
import pyjap.logger

from io import StringIO
import pandas as pd
import keyring as kr
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

class AzureBlobHandler:
    def __init__(
        self,
        environment: str = None,
        connection_string: str = None
    ):
        if environment is None and connection_string is None:
            logging.critical("One of environment and connection_string must be specified.")
            return
        elif connection_string is None:
            connection_string = kr.get_password(environment, "blob_connection_string")
        self.storage_client = BlobServiceClient.from_connection_string(connection_string)
        return
    
    def get_blob_names(self, container: str):
        with self.storage_client.get_container_client(container) as container_client:
            return container_client.list_blob_names()
        
    def get_blob_as_bytes(self, container: str, blob: str):
        blob_client = self.storage_client.get_blob_client(container, blob)
        return blob_client.download_blob().readall()
        
    def get_blob_as_string(self, container: str, blob: str, encoding: str = "utf-8"):
        return self.get_blob_as_bytes(container, blob).decode(encoding)
    
    def get_blob_csv_as_dataframe(self, container: str, blob:str):
        return pd.read_csv(StringIO(self.get_blob_as_string(container, blob)))