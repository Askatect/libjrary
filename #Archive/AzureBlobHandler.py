import logging
import pyjap.logger

import pyjap.utilities as utl

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
        self._connection_string = connection_string
        try:
            self._storage_client = BlobServiceClient.from_connection_string(connection_string)
        except Exception as error:
            logging.critical(f"Failed to connect to {str(self)}. " + str(error))
        else:
            logging.info(f"Successfully connected to {str(self)}.")
        return
    
    def __str__(self):
        return utl.extract_param(self._connection_string, 'AccountName=', ';')
    
    def get_blobs(self, container: str):
        with self._storage_client.get_container_client(container) as container_client:
            return container_client.list_blobs()
    
    def get_blob_names(self, container: str):
        with self._storage_client.get_container_client(container) as container_client:
            return container_client.list_blob_names()
        
    def get_blob_as_bytes(self, container: str, blob: str):
        blob_client = self._storage_client.get_blob_client(container, blob)
        return blob_client.download_blob().readall()
        
    def get_blob_as_string(self, container: str, blob: str, encoding: str = "utf-8"):
        return self.get_blob_as_bytes(container, blob).decode(encoding)
    
    def get_blob_csv_as_dataframe(self, container: str, blob: str):
        return pd.read_csv(StringIO(self.get_blob_as_string(container, blob)))
    
    def copy_blob(
        self, 
        source_container: str, 
        source_blob: str, 
        target_container: str = None, 
        target_blob: str = None
    ):
        if target_container is None:
            target_container = source_container
        if target_blob is None:
            target_blob = source_blob.replace('.', '_copy.')
        source_blob_client = self._storage_client.get_blob_client(source_container, source_blob)
        target_blob_client = self._storage_client.get_blob_client(target_container, target_blob)
        target_blob_client.start_copy_from_url(source_blob_client.url)
        return
    
    def delete_blob(self, container: str, blob: str):
        self._storage_client.get_blob_client(container, blob).delete_blob()
        return
    
    def rename_blob(
        self,
        container: str,
        old_blob: str,
        new_blob: str
    ):
        self.copy_blob(container, old_blob, container, new_blob)
        self.delete_blob(container, old_blob)
        return