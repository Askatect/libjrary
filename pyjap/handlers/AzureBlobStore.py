from pyjap.Logger import LOG

import pyjap.utilities as utl

import keyring as kr
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
    
class AzureBlobHandler:
    """
    AzureBlobHandler - A utility class for interacting with Azure Blob Storage.

    Args:
        environment (str, optional): The environment key for retrieving connection details.
        connection_string (str, optional): The full Azure Blob Storage connection string.

    Methods:
        get_blobs: Retrieves a list of blobs in a specified container.
        get_blob_names: Retrieves a list of blob names in a specified container.
        get_blob_as_bytes: Retrieves the content of a blob as bytes.
        get_blob_as_string: Retrieves the content of a blob as a string.
        get_blob_csv_as_dataframe: Retrieves the content of a CSV blob as a DataFrame.
        copy_blob: Copies a blob from one location to another.
        delete_blob: Deletes a blob from a container.
        rename_blob: Renames a blob within the same container.

    Use Case:
        handler = AzureBlobHandler(environment='your_environment')
        blobs = handler.get_blob_names(container='your_container')
        for blob in blobs:
            content = handler.get_blob_as_string(container='your_container', blob=blob)
            # Perform operations with blob content
    """
    def __init__( # Add parameters to this, a la SQLHandler.
        self,
        environment: str = None,
        connection_string: str = None
    ):
        """
        Initializes the AzureBlobHandler instance.

        Args:
            environment (str, optional): The environment key for retrieving connection details.
            connection_string (str, optional): The full Azure Blob Storage connection string.
        """
        if environment is None and connection_string is None:
            LOG.critical("One of environment and connection_string must be specified.")
            return
        elif connection_string is None:
            connection_string = kr.get_password(environment, "blob_connection_string")
        self._connection_string = connection_string
        LOG.info(f"Connecting to {str(self)}...")
        try:
            self._storage_client = BlobServiceClient.from_connection_string(connection_string)
        except Exception as error:
            LOG.critical(f"Failed to connect to {str(self)}. {str(error)}")
        else:
            LOG.info(f"Successfully connected to {str(self)}.")
        return
    
    def __str__(self):
        """
        Returns the Azure Storage Account name.

        Returns:
            str: The Azure Storage Account name.
        """
        return utl.extract_param(self._connection_string, 'AccountName=', ';')
    
    def get_blobs(self, container: str):
        """
        Retrieves a list of blobs in a specified container.

        Args:
            container (str): The name of the container.

        Returns:
            Iterable: An iterable containing blob information.
        """
        LOG.info(f"Getting list of blobs from {container} in {str(self)}.")
        with self._storage_client.get_container_client(container) as container_client:
            return container_client.list_blobs()
    
    def get_blob_names(self, container: str):
        """
        Retrieves a list of blob names in a specified container.

        Args:
            container (str): The name of the container.

        Returns:
            Iterable: An iterable containing blob names.
        """
        LOG.info(f'Getting list of blob names from "{container}" in {str(self)}.')
        with self._storage_client.get_container_client(container) as container_client:
            return container_client.list_blob_names()
        
    def get_blob_as_bytes(self, container: str, blob: str):
        """
        Retrieves the content of a blob as bytes.

        Args:
            container (str): The name of the container.
            blob (str): The name of the blob.

        Returns:
            bytes: The content of the blob.
        """
        LOG.info(f'Retrieving "{blob}" from "{container}" in {str(self)} as bytes.')
        blob_client = self._storage_client.get_blob_client(container, blob)
        return blob_client.download_blob().readall()
        
    def get_blob_as_string(self, container: str, blob: str, encoding: str = "utf-8"):
        """
        Retrieves the content of a blob as a string.

        Args:
            container (str): The name of the container.
            blob (str): The name of the blob.
            encoding (str, optional): The encoding to use. Default is 'utf-8'.

        Returns:
            str: The content of the blob as a string.
        """
        LOG.info(f'Decoding "{blob}" from "{container}" in {str(self)} via {encoding} encoding.')
        return self.get_blob_as_bytes(container, blob).decode(encoding)
    
    def get_blob_csv_as_dataframe(self, container: str, blob: str, encoding: str = "utf-8", header: int = 0):
        """
        Retrieves the content of a CSV blob as a DataFrame.

        Args:
            container (str): The name of the container.
            blob (str): The name of the CSV blob.
            encoding (str, optional): The encoding to use. Default is 'utf-8'.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the CSV data.
        """
        LOG.info(f'Loading "{blob}" from "{container}" in {str(self)} into a dataframe.')
        return pd.read_csv(StringIO(self.get_blob_as_string(container, blob, encoding)), header = header)
    
    def copy_blob(
        self, 
        source_container: str, 
        source_blob: str, 
        target_container: str = None, 
        target_blob: str = None
    ):
        """
        Copies a blob from one location to another.

        Args:
            source_container (str): The source container name.
            source_blob (str): The source blob name.
            target_container (str, optional): The target container name. Defaults to source_container.
            target_blob (str, optional): The target blob name. Defaults to source_blob.replace('.', '_copy.') if in same container.
        """
        if target_container is None:
            target_container = source_container
        if target_blob is None and target_container == source_container:
            target_blob = source_blob.replace('.', '_copy.')
        LOG.info(f'Copying "{source_blob}" from "{source_container}" to "{target_blob}" in "{target_container}" at {str(self)}.')
        source_blob_client = self._storage_client.get_blob_client(source_container, source_blob)
        target_blob_client = self._storage_client.get_blob_client(target_container, target_blob)
        target_blob_client.start_copy_from_url(source_blob_client.url)
        return
    
    def delete_blob(self, container: str, blob: str):
        """
        Deletes a blob from a container.

        Args:
            container (str): The name of the container.
            blob (str): The name of the blob.
        """
        LOG.info(f'Deleting "{blob}" from "{container}" in {str(self)}.')
        self._storage_client.get_blob_client(container, blob).delete_blob()
        return
    
    def rename_blob(
        self,
        container: str,
        old_blob: str,
        new_blob: str
    ):
        """
        Renames a blob within the same container.

        Args:
            container (str): The name of the container.
            old_blob (str): The current name of the blob.
            new_blob (str): The new name for the blob.
        """
        self.copy_blob(container, old_blob, container, new_blob)
        self.delete_blob(container, old_blob)
        return