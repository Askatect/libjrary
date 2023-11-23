"""
Module: pyjap.sqlhandler

This module provides utility classes for handling SQL Server connections and operations, interacting with Azure Blob Storage, and sending emails.

Classes:
    - SQLHandler: A utility class for handling SQL Server connections and operations.
    - AzureBlobHandler: A utility class for interacting with Azure Blob Storage.
    - EmailHandler: A utility class for sending emails.

Dependencies:
    - pyodbc
    - pandas
    - keyring
    - pyjap.utilities
    - io.StringIO
    - azure.storage.blob
    - smtplib
    - email.mime.multipart.MIMEMultipart
    - email.mime.text.MIMEText
"""

import logging
from pyjap.logger import Logger
log = Logger()

import pyjap.utilities as utl

import pandas as pd
import keyring as kr

import pyodbc

class SQLHandler:
    """
    SQLHandler - A utility class for handling SQL Server connections and operations.

    Args:
        connection_string (str, optional): The full SQL Server connection string.
        environment (str, optional): The environment key for retrieving connection details.
        driver (str, optional): The ODBC driver name. Default is '{SQL Server}'.
        server (str, optional): The SQL Server address.
        port (int, optional): The port number for the SQL Server connection.
        database (str, optional): The name of the database.
        uid (str, optional): The user ID for authentication.
        pwd (str, optional): The password for authentication.
        encrypt (str, optional): Flag indicating whether to use encryption ('yes' or 'no'). Default is 'yes'.
        trust_server_certificate (str, optional): Flag indicating trust for the server certificate ('yes' or 'no'). Default is 'no'.
        connection_timeout (int, optional): Connection timeout in seconds. Default is 30.

    Attributes:
        connected (bool): Flag indicating if a connection is currently open.
        conn: The pyodbc connection object.
        cursor: The pyodbc cursor object.

    Methods:
        connect_to_mssql: Establishes a connection to the SQL Server.
        rollback: Rolls back the current transaction.
        commit: Commits the current transaction.
        close_connection: Closes the open connection.
        select_to_dataframe: Executes a SELECT query and returns the result as a DataFrame.
        execute_query: Executes a SQL query.
        insert: Inserts data into the specified table.
        create_table: Creates a new table in the database.

    Usage:
        handler = SQLHandler(connection_string='your_connection_string')
        handler.connect_to_mssql()
        handler.select_to_dataframe('SELECT * FROM your_table')
        handler.close_connection()
    """
    def __init__(
        self,
        connection_string: str = None,
        environment: str = None,
        driver: str = '{SQL Server}',
        server: str = None,
        port: int = None,
        database: str = None,
        uid: str = None,
        pwd: str = None,
        encrypt: str = 'yes',
        trust_server_certificate: str = 'no',
        connection_timeout: int = 30
    ):
        """
        Initializes the SQLHandler instance.

        Args:
            connection_string (str, optional): The full SQL Server connection string.
            environment (str, optional): The environment key for retrieving connection details.
            driver (str, optional): The ODBC driver name. Default is '{SQL Server}'.
            server (str, optional): The SQL Server address.
            port (int, optional): The port number for the SQL Server connection.
            database (str, optional): The name of the database.
            uid (str, optional): The user ID for authentication.
            pwd (str, optional): The password for authentication.
            encrypt (str, optional): Flag indicating whether to use encryption ('yes' or 'no'). Default is 'yes'.
            trust_server_certificate (str, optional): Flag indicating trust for the server certificate ('yes' or 'no'). Default is 'no'.
            connection_timeout (int, optional): Connection timeout in seconds. Default is 30.
        """
        if connection_string is None and environment is None and (server is None or database is None):
            logging.critical("Not enough information given to start SQLHandler.")
            return
        self._connection_string = connection_string
        self._params = {
            'driver': driver,
            'server': server,
            'port': str(port),
            'database': database,
            'uid': uid,
            'pwd': pwd,
            'encrypt': encrypt,
            'trustservercertificate': trust_server_certificate,
            'connectiontimeout': str(connection_timeout)
        }
        self.connected = False
        self.conn = None
        if self._connection_string is not None:
            for param in [param for param, value in self._params.items() if value is None]:
                self._params[param] = utl.extract_param(self._connection_string, param+'=', ';')
        elif environment is not None:
            for param in [param for param, value in self._params.items() if value is None]:
                self._params[param] = kr.get_password(environment, param)

        if self._connection_string is None:
            self._connection_string = ""
            for param, value in [(param, value) for param, value in self._params.items() if value is not None]:
                self._connection_string += f"{param}={value};"
        return
    
    def __str__(self):
        """
        Returns a string representation of the connected server and database.

        Returns:
            str: A string in the format '[server].[database]'.
        """
        return f'[{self._params["server"]}].[{self._params["database"]}]'
    
    def _schema_table_to_object_name(self, schema: str, table: str):
        """
        Converts schema and table names to a formatted object name.

        Args:
            schema (str): The schema name.
            table (str): The table name.

        Returns:
            str: The formatted object name.
        """
        schema = '' if schema is None or schema == '' else '[' + schema + '].'
        return f"{schema}[{table}]"
    
    def connect_to_mssql(self, auto_commit: bool = False):
        """
        Establishes a connection to the SQL Server.
        """
        if self.connected:
            logging.error(f"Connection to {str(self)} already open.")
            return
        logging.info(f'Attempting to connect to [{self._params["server"]}].[{self._params["database"]}]...')
        try:
            self.conn = pyodbc.connect(self._connection_string, autocommit = auto_commit)
            self.cursor = self.conn.cursor()
        except pyodbc.InterfaceError as error:
            logging.error(f'Failed to connect to [{self._params["server"]}].[{self._params["database"]}]. {error}. Available drivers: {pyodbc.drivers()}.')
            raise
        except Exception as error:
            logging.error(f'Failed to connect to [{self._params["server"]}].[{self._params["database"]}]. {error}')
            raise
        else:
            self.connected = True
            logging.info(f"Successfully connected to {str(self)}.")
            return self.cursor
    
    def rollback(self):
        """
        Rolls back the current transaction.
        """
        if not self.connected:
            logging.error("No open connection to roll back.")
            return
        logging.info(f"Rolling back transaction to {str(self)}.")
        self.conn.rollback()
        return
    
    def commit(self):
        """
        Commits the current transaction.
        """
        if not self.connected:
            logging.error("No open connection to commit.")
            return
        logging.info(f"Committing transaction to {str(self)}.")
        self.conn.commit()
        return

    def close_connection(self, commit: bool = True):
        """
        Closes the open connection.

        Args:
            commit (bool, optional): Flag indicating whether to commit changes before closing. Default is True.
        """
        if not self.connected:
            logging.error("No open connection to close.")
            return
        if commit:
            self.commit()
        else:
            self.rollback()
        self.cursor.close()
        self.conn.close()
        self.connected = False
        logging.info(f"Closed connection to {str(self)}.")
        return
    
    def select_to_dataframe(self, query: str):
        """
        Executes a SELECT query and returns the result as a DataFrame.

        Args:
            query (str): The SELECT query to execute.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the query result.
        """
        if not self.connected:
            try:
                self.cursor = self.connect_to_mssql(auto_commit = False)
            except Exception as error:
                logging.error(f"Could not connect to {self}. {error}.")
                return
        selection = pd.read_sql_query(query, con=self.conn)
        self.close_connection(commit = False)
        return selection
    
    def execute_query(self, query: str, values: tuple = None, commit: bool = True) -> None|list[pyodbc.Row]:
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query to execute.
            values (tuple, optional): The parameter values for the query.
        """
        if not self.connected:
            try:
                self.cursor = self.connect_to_mssql(auto_commit = commit)
            except Exception as error:
                logging.error(f"Could not connect to {self}. {error}.")
                return
        logging.info(f"Running script against {str(self)}:\n{query}")
        if values is None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, (values))
        try:
            selection = self.cursor.fetchall()
        except:
            selection = None
        self.close_connection(commit)
        return selection

    def insert(
        self, 
        schema: str,  
        table: str, 
        cols: list[str] = None, 
        values: list[list|tuple] = None,
        df: pd.DataFrame = None,
        fast_execute: bool = True,
        auto_create_table: bool = True,
        replace_table: bool = False,
        commit: bool = True
    ):
        """
        Inserts data into the specified table.

        Args:
            schema (str): The schema of the target table.
            table (str): The name of the target table.
            cols (list, optional): The list of column names for the insertion.
            values (list[list|tuple], optional): The list of values to insert.
            df (pd.DataFrame, optional): A DataFrame containing data to insert.
            fast_execute (bool, optional): Flag indicating whether to use fast executemany. Default is True.
            auto_create_table (bool, optional): Flag indicating whether to auto-create the table if not exists. Default is True.
            replace_table (bool, optional): Flag indicating whether to replace the existing table. Default is False.
        """
        if df is None and values is None:
            logging.error("No values given to insert.")
            return
        elif df is not None:
            cols = df.columns.to_list()
            values = df.values.tolist() 
        elif len(set([len(vals) for vals in values])) != 1:
            logging.error("Value vectors must all be the same size.")
            return
        
        if not self.connected:
            try:
                self.connect_to_mssql(auto_commit = commit)
            except Exception as error:
                logging.error(f"Could not connect to {self}. {error}.")
                return None
        
        if auto_create_table:
            self.create_table(table, cols, schema = schema, replace = replace_table, commit = commit)
        object_name = self._schema_table_to_object_name(schema, table)

        try:
            logging.info(f"Inserting into {object_name} at {str(self)}...")
            self.connect_to_mssql(commit)
            self.cursor.fast_executemany = fast_execute
            cmd = f"INSERT INTO {object_name}{'([' + '], ['.join(cols) + '])' if len(cols) > 0 else ''} VALUES ({'?' + (len(values[0]) - 1)*', ?'})"
            try:
                self.cursor.executemany(cmd, values)
            except Exception as error:
                if fast_execute:
                    logging.error(f"Insert failed. {error}.\nAttempting insert without fast_executemany...")
                    self.cursor.fast_executemany = False
                    self.cursor.executemany(cmd, values)
        except Exception as error:
            self.close_connection(commit = False)
            logging.error(f"Insert failed. Error: {error}.")
        else:
            logging.info(f"Insert complete!")

        self.close_connection(commit)
        return
    
    def create_table(
        self,
        table: str, 
        columns: list[str] = [], 
        datatypes: list[str] = [], 
        schema: str = None, 
        replace: bool = False,
        commit: bool = True
    ):
        """
        Creates a new table in the database.

        Args:
            table (str): The name of the table to create.
            columns (list, optional): List of column names.
            datatypes (list, optional): List of column data types.
            schema (str, optional): The schema of the table.
            replace (bool, optional): Flag indicating whether to replace the existing table. Default is False.
        """
        object_name = self._schema_table_to_object_name(schema, table)
        if not replace:
            result = self.execute_query(f"SELECT 1 FROM sys.tables WHERE [object_id] = OBJECT_ID('{object_name}')")[0][0]
            if result is not None:
                logging.info(f"The table {object_name} already exists.")
                return
            cmd = ""
        else:
            cmd = f"DROP TABLE IF EXISTS {object_name};\n"
        col_count = len(columns)
        datatype_count = len(datatypes)
        if datatype_count >= col_count:
            datatypes = datatypes[:col_count]
        else:
            max_length = self.execute_query("SELECT CONVERT(int, [max_length]/2) FROM sys.types WHERE [system_type_id] = 231")[0][0]
            datatypes.extend([f'nvarchar({max_length})']*(col_count - datatype_count))
        column_definition = ',\n\t'.join(f"[{col}] {datatype}" for col, datatype in zip(columns, datatypes))
        cmd += f"CREATE TABLE {object_name} (\n\t{column_definition})"
        try:
            self.execute_query(cmd, commit = commit)
        except Exception as error:
            self.close_connection(commit = False)
            logging.error(f"Failed to create table {object_name} at {str(self)}. Error: {error}.")
        else:
            logging.info(f"Successfully created table {object_name} at {str(self)}.")
        return
    
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
    def __init__(
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
            logging.critical("One of environment and connection_string must be specified.")
            return
        elif connection_string is None:
            connection_string = kr.get_password(environment, "blob_connection_string")
        self._connection_string = connection_string
        logging.info(f"Connecting to {str(self)}...")
        try:
            self._storage_client = BlobServiceClient.from_connection_string(connection_string)
        except Exception as error:
            logging.critical(f"Failed to connect to {str(self)}. " + str(error))
        else:
            logging.info(f"Successfully connected to {str(self)}.")
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
        return self.get_blob_as_bytes(container, blob).decode(encoding)
    
    def get_blob_csv_as_dataframe(self, container: str, blob: str, encoding: str = "utf-8"):
        """
        Retrieves the content of a CSV blob as a DataFrame.

        Args:
            container (str): The name of the container.
            blob (str): The name of the CSV blob.
            encoding (str, optional): The encoding to use. Default is 'utf-8'.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the CSV data.
        """
        return pd.read_csv(StringIO(self.get_blob_as_string(container, blob, encoding)))
    
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
            target_blob (str, optional): The target blob name. Defaults to source_blob.replace('.', '_copy.').
        """
        if target_container is None:
            target_container = source_container
        if target_blob is None:
            target_blob = source_blob.replace('.', '_copy.')
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
    
import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

class EmailHandler:
    """
    EmailHandler - A utility class for sending emails.

    Args:
        environment (str, optional): The environment key for retrieving connection details.
        email (str, optional): The email address.
        password (str, optional): The email account password.
        smtp (str, optional): The SMTP server address.
        port (int, optional): The SMTP server port.

    Attributes:
        params (dict): A dictionary containing the email connection parameters.
        connected (bool): Flag indicating if a connection to the SMTP server is currently open.
        connection: The smtplib.SMTP connection object.

    Methods:
        connect_to_smtp: Connects to the SMTP server.
        send_email: Sends an email with optional HTML content.
        close_connection: Closes the SMTP connection.

    Usage:
        handler = EmailHandler(environment='your_environment')
        handler.connect_to_smtp()
        handler.send_email(to='recipient@example.com', subject='Test Email', body_alt_text='This is a test email.')
        handler.close_connection()
    """
    def __init__(
            self,
            environment: str = None,
            email: str = None,
            password: str = None,
            smtp: str = None,
            port: int = None
    ):
        """
        Initializes the EmailHandler instance.

        Args:
            environment (str, optional): The environment key for retrieving connection details.
            email (str, optional): The email address.
            password (str, optional): The email account password.
            smtp (str, optional): The SMTP server address.
            port (int, optional): The SMTP server port.
        """
        self.params = {
            'email': email,
            'password': password,
            'smtp': smtp,
            'port': port
        }
        if environment is not None:
            for param in self.params.keys():
                value = kr.get_password(environment, param)
                if value is not None:
                    self.params[param] = value
        self.connected = False
        return
    
    def __str__(self):
        return self.params['email']
    
    def connect_to_smtp(self):
        """
        Connects to the SMTP server.
        """
        if self.connected:
            logging.error("Connection already open.")
            return
        logging.info(f"Attempting to connect to {self}.")
        try:
            self.connection = smtplib.SMTP(self.params['smtp'], self.params['port'])
            self.connection.starttls()
            self.connection.login(user = self.params['email'], password = self.params['password'])
        except Exception as error:
            logging.error(f"Failed to connect to {self.params['smtp']}. {error}.")
        else:
            self.connected = True
            logging.info(f"Successfully connected to {self.params['smtp']}.")
        return
    
    def send_email(self, 
                   to: list|str, 
                   cc: list|str = [], 
                   bcc: list|str = [], 
                   subject: str = "",
                   body_html: str = None,
                   body_alt_text: str = None,
                   attachments: list[str]|str = []
    ):
        """
        Sends an email.

        Args:
            to (list|str): The recipient(s) of the email.
            cc (list|str, optional): The carbon copy recipient(s) of the email.
            bcc (list|str, optional): The blind carbon copy recipient(s) of the email.
            subject (str): The subject of the email.
            body_html (str, optional): The HTML body of the email.
            body_alt_text (str, optional): The alternative plain text body of the email.
            attachments: (list[str]|str): File address(es) to add as email attachments.
        """
        if not self.connected:
            try:
                self.connect_to_smtp()
            except:
                logging.error(f"Could not connect to {self}. {error}.")
                return None
            
        logging.info(f"Attempting to send email from {self.params['email']}...")
        message = MIMEMultipart()
        message["From"] = self.params['email']
        message["Subject"] = subject
        
        if type(to) is str:
            to = [to]
        message["To"] = ",".join(to)
        
        if type(cc) is str:
            cc = [cc]
        message["Cc"] = ",".join(cc)
        
        if type(bcc) is str:
            bcc = [bcc]
        
        message.attach(MIMEText(body_alt_text, "plain"))
        
        if type(attachments) is str:
            attachments = [attachments]
        for file in attachments:
            try:
                with open(file, 'rb') as file_reader:
                    part = MIMEApplication(file_reader.read(), name = basename(file))
                part['Content-Disposition'] = f'attachment; filename={basename(file)}'
                message.attach(part)
            except Exception as error:
                logging.error(f'Could not attach file "{file}".')
                return None
        
        if body_html is not None:
            message.attach(MIMEText(body_html, "html"))
        try:
            logging.info(f"Sending email from {self}...")
            self.connection.sendmail(self.params['email'], to + cc + bcc, message.as_string())
        except Exception as error:
            logging.error(f"Failed to send email. {error}.")
        else:
            logging.info("Email sent successfully!")
        self.close_connection()
        return
            
    def close_connection(self):
        """
        Closes the SMTP connection.
        """
        if not self.connected:
            logging.error("No open connection.")
            return
        try:
            self.connection.quit()
        except:
            logging.error("Failed to close connection.")
        else:
            self.connected = False
            logging.info(f"Closed connection to {self}.")
        return