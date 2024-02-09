"""
# sql.py

Version: 1.1
Authors: JRA
Date: 2024-02-08

Explanation:
Contains the sqlhandler class for operating on SQL Server databases.

Requirements:
- pyjap.logger.LOG: For logging.
- pyjap.utilities.extract_param: For reading parameter values from connection strings.
- pandas: For DataFrames.
- keyring: For storing and retrieving of keys.
- pyodbc: To interface with the database.

Artefacts:
- SQLHandler (class): Operates on SQL Server databases.

Usage:
>>> from pyjap.sql import SQLHandler

History:
- 1.1 JRA (2024-02-09): Added retry_wait.
- 1.0 JRA (2024-02-08): Initial version.
"""
from pyjap.logger import LOG

from pyjap.utilities import extract_param

import pandas as pd
import keyring as kr
import pyodbc
from time import sleep

class SQLHandler:
    """
    ## SQLHandler
        
    Version: 1.1
    Authors: JRA
    Date: 2024-02-09

    Explanation:
    Operates on SQL Server databases.

    Artefacts:
    - __connection_string (str): Connection string.
    - __params (dict): Connection parameters.
    - connected (bool): If true, indicates a successful active connection.
    - conn (pyodbc.Connection): Connection object.
    - description (Tuple[Tuple[str, Any, int, int, int, int, bool]]): Of a pyodbc connection:
        - Name of the column or alias.
        - Type code, the Python-equivalent class of the column, such as  str for varchar.
        - Display size (pyodbc does not set this value).
        - Internal size in bytes.
        - Precision
        - Scale
        - Nullability.
    - cursor (pyodbc.Cursor)
    - __init__ (func): Initialises the handler.
    - __str__ (func): Returns the server and database of the handler.
    - __bool__ (func): Returns True when the handler is successfully connected.
    - __schema_table_to_object_name (func): Standardises a given schema and object to a bracket wrapped, stop separated string.
    - connect_to_mssql (func): Establishes a connection to the SQL Server.
    - rollback (func): Rolls back the current transaction.
    - commit (func): Commits the current transaction.
    - close_connection (func): Closes the open connection.
    - select_to_dataframe (func): Executes a DQL script and returns the result as a DataFrame.
    - execute_query (func): Executes a SQL query.
    - query_columns (func): Retrieves the columns of a query or the most recently executed query.
    - insert (func): Inserts data into a specified table.
    - create_table (func): Creates a table in the database.

    Usage:
    >>> executor = SQLHandler(environment = 'dev')
    >>> executor.execute_query("SELECT 'value' AS [column]")
    [('value')]
    >>> executor.query_columns()
    ['column']

    History:
    - 1.1 JRA (2024-02-09): Added retry_wait.
    - 1.0 JRA (2024-02-09): Initial version.
    """
    def __init__(
        self,
        connection_string: str = None,
        environment: str = None,
        driver: str = None,
        server: str = None,
        port: int = None,
        database: str = None,
        uid: str = None,
        pwd: str = None,
        encrypt: str = 'yes',
        trust_server_certificate: str = 'no',
        connection_timeout: int = 30,
        retry_wait: int = None
    ):
        """
        ### __init__

        Version: 1.1
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Initialises the handler.

        Parameters:
        - connection_string (str): The connection string to use. Defaults to environment or other parameters if not supplied.
        - environment (str): The environment to get keyring keys from. Defaults to other parameters if not supplied.
        - driver (str): The ODBC driver to use. No default driver.
        - server (str): The server to connect to.
        - port (int): The port to connect via.
        - database (str): The database to connect to.
        - uid (str): The user identifier.
        - pwd (str): The user password.
        - encrypt (str): If 'yes', encryption is used.
        - connection_timeout (int): Timeout limit to use during connections.
        - retry_wait (int): If populated, connections to the database are retried once on failure after this number of seconds. Defaults to no retry.

        Usage:
        >>> executor = SQLHandler(environment = 'dev')

        History:
        - 1.1 JRA (2024-02-09): Added retry_wait.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if connection_string is None and environment is None and (server is None or database is None):
            LOG.critical("Not enough information given to start SQLHandler.")
            return
        self.__connection_string = connection_string
        self.__params = {
            'driver': driver,
            'server': server,
            'port': port,
            'database': database,
            'uid': uid,
            'pwd': pwd,
            'encrypt': encrypt,
            'trustservercertificate': trust_server_certificate,
            'connectiontimeout': connection_timeout
        }
        self.connected = False
        self.conn = None
        self.description = ()
        self.retry_wait = retry_wait
        if self.__connection_string is not None:
            LOG.info("Reading parameters from connection string.")
            for param in [param for param, value in self.__params.items() if value is None]:
                self.__params[param] = extract_param(self.__connection_string, param+'=', ';')
        elif environment is not None:
            LOG.info("Getting parameters from keyring.")
            for param in [param for param, value in self.__params.items() if value is None]:
                value = kr.get_password(environment, param)
                if value is None:
                    LOG.warning(f'No value found for {param} in {environment} environment.')
                else:
                    self.__params[param] = value

        if self.__connection_string is None:
            self.__connection_string = ""
            for param, value in [(param, value) for param, value in self.__params.items() if value is not None]:
                self.__connection_string += f"{param}={value};"
        return
    
    def __str__(self) -> str:
        """
        ### __str__

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Returns the server and database of the handler.

        Returns:
        - (str)

        Usage:
        >>> print(executor)
        "[server].[database]"

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        # To return detailed information, could set this to return the following format:
        # [SQL(server='server', database='database', etc...)]
        return f'[{self.__params["server"]}].[{self.__params["database"]}]'
    
    def __bool__(self) -> bool:
        """
        ### __bool__

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Returns True when the handler is successfully connected.

        Returns:
        - self.connected (bool)

        Usage:
        >>> bool(executor)

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        return self.connected
    
    def __schema_table_to_object_name(self, schema: str, table: str) -> str:
        """
        Converts schema and table names to a formatted object name.

        Args:
            schema (str): The schema name.
            table (str): The table name.

        Returns:
            str: The formatted object name.

        ### __schema_table_to_object_name

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Standardises a given schema and object to a bracket wrapped, stop separated string.

        Parameters:
        - schema (str)
        - table (str)

        Returns:
        - (str)

        Usage:
        >>> executor.__schema_table_to_object_name('schema', 'table')
        '[schema].[table]'

        Tasklist:
        - What if the inputs are already wrapped?

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        schema = '' if schema is None or schema == '' else '[' + schema + '].'
        return f"{schema}[{table}]"
    
    def connect_to_mssql(self, auto_commit: bool = False, retry_wait: int = None) -> pyodbc.Cursor|None:
        """
        ### connect_to_mssql

        Version: 1.1
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Establishes a connection to the SQL Server.

        Parameters:
        - auto_commit (bool): If true, transactions are committed by default. Default is false.
        - retry_wait (int): If populated, connections to the database are retried once on failure after this number of seconds. Defaults to no retry.

        Returns:
        - self.cursor (pyodbc.Cursor)

        Usage:
        >>> executor.connect_to_mssql()
        <executor.cursor>

        History:
        - 1.1 JRA (2024-02-09): Added retry_wait.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if self.connected:
            LOG.error(f"Connection to {str(self)} already open.")
            return
        LOG.info(f'Attempting to connect to {self}...')
        try:
            self.conn = pyodbc.connect(self.__connection_string, autocommit = auto_commit)
            self.cursor = self.conn.cursor()
        except:
            try:
                sleep(retry_wait or self.retry_wait)
                self.conn = pyodbc.connect(self.__connection_string, autocommit = auto_commit)
                self.cursor = self.conn.cursor()
            except pyodbc.InterfaceError as error:
                LOG.error(f'Failed to connect to {self}. {error}. Available drivers: {pyodbc.drivers()}.')
                raise
            except Exception as error:
                LOG.error(f'Failed to connect to {self}. {error}')
                raise
        else:
            self.connected = True
            LOG.info(f"Successfully connected to {str(self)}.")
            return self.cursor
    
    def rollback(self):
        """
        ### rollback

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Rolls back transactions.

        Usage:
        >>> executor.rollback()

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if not self.connected:
            LOG.warning("No open connection to roll back.")
            return
        LOG.info(f"Rolling back transaction to {str(self)}.")
        self.conn.rollback()
        return
    
    def commit(self):
        """
        ### commit

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Commits transactions.

        Usage:
        >>> executor.commit()

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if not self.connected:
            LOG.warning("No open connection to commit.")
            return
        LOG.info(f"Committing transaction to {str(self)}.")
        self.conn.commit()
        return

    def close_connection(self, commit: bool = True):
        """
        ### close_connection

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Closes the open connection.

        Requirements:
        - SQLHandler.commit
        - SQLHandler.rollback

        Parameters:
        - commit (bool): If true, transaction is committed. Defaults to true.

        Usage:
        >>> executor.close_connection()

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if not self.connected:
            LOG.error("No open connection to close.")
            return
        if commit:
            self.commit()
        else:
            self.rollback()
        self.description = self.cursor.description
        self.cursor.close()
        self.conn.close()
        self.connected = False
        LOG.info(f"Closed connection to {str(self)}.")
        return
    
    def select_to_dataframe(self, query: str, values: tuple = None) -> pd.DataFrame:
        """
        ### select_to_dataframe

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Executes a DQL script and returns the result as a DataFrame.

        Requirements:
        - SQLHandler.connect_to_mssql

        Parameters:
        - query (str): The query to run.
        - values (tuple): The values to substitute into the query. Defaults to None.

        Returns:
        - (pandas.DataFrame): Output of query.

        Usage:
        >>> executor.select_to_dataframe("SELECT 'value' AS [column]")
        <pandas.DataFrame>

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if not self.connected:
            try:
                self.cursor = self.connect_to_mssql(auto_commit = False)
            except Exception as error:
                LOG.error(f"Could not connect to {self}. {error}.")
                return
        LOG.info(f"Generating dataframe against {str(self)}.")
        results = self.execute_query(query, values, commit = False)
        # selection = pd.read_sql_query(query, con=self.conn)
        # selection = pd.DataFrame.from_records(results, columns = self.query_columns())
        return pd.DataFrame((tuple(row) for row in results), columns = self.query_columns())
    
    def execute_query(self, query: str, values: tuple = None, commit: bool = True) -> None|list[pyodbc.Row]:
        """
        ### execute_query

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Executes a SQL query.

        Requirements:
        - SQLHandler.connect_to_mssql
        - SQLHandler.close_connection

        Parameters:
        - query (str): The query to run.
        - values (tuple): The values to substitute into the query. Defaults to None.
        - commit (bool): If true, the query is committed.

        Returns:
        - selection (None|list[pyodbc.Row]): The output selection of the query.

        Usage:
        >>> executor.execute_query("SELECT 'value' AS [column]")

        History: 
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if not self.connected:
            try:
                self.cursor = self.connect_to_mssql(auto_commit = commit)
            except Exception as error:
                LOG.error(f"Could not connect to {self}. {error}.")
                return
        LOG.info(f"Running script against {str(self)}:\n{query}\nValues: {values}.")
        if values is None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, (values))
        try:
            selection = self.cursor.fetchall()
        except Exception as error:
            LOG.error(f"Error occurred during fetch. {error}")
            selection = None
        self.close_connection(commit)
        return selection
    
    def query_columns(self, query: str = None, values: tuple = None):
        """
        ### query_columns

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Retrieves the columns of a query or the most recently executed query.

        Requirements:
        - SQLHandler.execute_query

        Parameters:
        - query (str): The query to execute if needed. Defaults to the previous query.
        - values (tuple): The values to substitute into the query. Defaults to previous query.

        Returns:
        - cols (list[str]): A list of columns of the output of the select query.

        Usage:
        >>> executor.query_columns()
        ['column']

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if query is not None:
            self.execute_query(query, values, commit = False)

        try:
            cols = [col[0] for col in self.description]
        except Exception as error:
            LOG.warning(f"No query description available from {self}. {error}")
            return None
        else:
            return cols

    def insert(
        self, 
        schema: str,  
        table: str, 
        cols: list[str] = None, 
        values: list[list|tuple] = None,
        df: pd.DataFrame = None,
        prescript: str = None,
        postscript: str = None,
        fast_execute: bool = True,
        auto_create_table: bool = True,
        replace_table: bool = False,
        commit: bool = True
    ):
        """
        ### insert

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation:
        Inserts data into a specified table.

        Requirements:
        - SQLHandler.connect_to_mssql
        - SQLHandler.create_table
        - SQLHandler.close_connection

        Parameters:
        - schema (str): The schema of the object to insert to.
        - table (str): The table to insert to.
        - cols (list[str]): The columns to insert to. Defaults to all columns of existing table. Defaults to None.
        - values (list[list|tuple]): The values to be inserted. Defaults to None.
        - df (pandas.DataFrame): Instead of providing columns and values, a dataframe can be used for insert. Defaults to None.
        - prescript (str): A script to run prior to the insert. Defaults to None.
        - postcript (str): A script to run after the insert. Defaults to None.
        - fast_execute (bool): If true, fast execute is utilised. Failed inserts are retried without fast execute. Defaults to true.
        - auto_create_table (bool): If true, the table is created if it does not already exist. Defaults to true.
        - replace_table (bool): If true, the table is replaced if it already exists. Defaults to false.
        - commit (bool): If true, the insert is committed. Defaults to true.

        Usage:
        >>> executor.insert('schema', 'table', df)

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if df is None and values is None:
            LOG.error("No values given to insert.")
            return
        elif df is not None:
            cols = df.columns.to_list()
            values = df.values.tolist() 
        elif len(set([len(vals) for vals in values])) != 1:
            LOG.error("Value vectors must all be the same size.")
            return
        
        if not self.connected:
            try:
                self.connect_to_mssql(auto_commit = commit)
            except Exception as error:
                LOG.error(f"Could not connect to {self}. {error}.")
                return
        
        if auto_create_table:
            if not self.create_table(table, cols, schema = schema, replace = replace_table, commit = commit):
                LOG.error(f"Could not create table for insert.")
                return
        object_name = self.__schema_table_to_object_name(schema, table)

        try:
            LOG.info(f"Inserting into {object_name} at {str(self)}...")
            self.connect_to_mssql(commit)
            if prescript is not None:
                LOG.info(f"Running script against {str(self)}:\n{prescript}")
                self.cursor.execute(prescript)
            self.cursor.fast_executemany = fast_execute
            cmd = f"INSERT INTO {object_name}{'([' + '], ['.join(cols) + '])' if len(cols) > 0 else ''} VALUES ({'?' + (len(values[0]) - 1)*', ?'})"
            try:
                self.cursor.executemany(cmd, values)
            except Exception as error:
                if fast_execute:
                    LOG.error(f"Insert failed. {error}.\nAttempting insert without fast_executemany...")
                    self.cursor.fast_executemany = False
                    self.cursor.executemany(cmd, values)
            if postscript is not None:
                LOG.info(f"Running script against {str(self)}:\n{postscript}")
                self.cursor.execute(postscript)
        except Exception as error:
            self.close_connection(commit = False)
            LOG.error(f"Insert failed. Error: {error}.")
        else:
            LOG.info(f"Insert complete!")
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
    ) -> bool:
        """
        ### create_table

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        Explanation: 
        Creates a table in the database.

        Requirements:
        - SQLHandler.__schema_table_to_object_name
        - SQLHandler.execute_query
        - SQLHandler.close_connection

        Parameters:
        - table (str): The name of the table.
        - columns (list[str]): The columns of the table.
        - datatypes (list[str]): The datatypes of the columns.
        - schema (str): The schema of the table. Defaults to None.
        - replace (bool): If true, if the table name already exists, then that table is dropped first. Defaults to False.
        - commit (bool): If true, the transaction is committed. Defaults to True.

        Returns:
        - (bool): True if the transaction passed without issues.

        Usage:
        >>> executor.create_table('table', ['column'], ['varchar(16)'])

        History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        object_name = self.__schema_table_to_object_name(schema, table)
        if not replace:
            try:
                result = self.execute_query(f"SELECT 1 FROM sys.tables WHERE [object_id] = OBJECT_ID('{object_name}')", commit = False)[0][0]
            except IndexError:
                result = None
            if result is not None:
                LOG.info(f"The table {object_name} already exists.")
                return 1
            cmd = ""
        else:
            cmd = f"DROP TABLE IF EXISTS {object_name};\n"
        col_count = len(columns)
        datatype_count = len(datatypes)
        if datatype_count >= col_count:
            datatypes = datatypes[:col_count]
        else:
            max_length = self.execute_query("SELECT CONVERT(int, [max_length]/2) FROM sys.types WHERE [system_type_id] = 231", commit = False)[0][0]
            datatypes.extend([f'nvarchar({max_length})']*(col_count - datatype_count))
        column_definition = ',\n\t'.join(f"[{col}] {datatype}" for col, datatype in zip(columns, datatypes))
        cmd += f"CREATE TABLE {object_name} (\n\t{column_definition}\n)"
        try:
            self.execute_query(cmd, commit = commit)
        except Exception as error:
            self.close_connection(commit = False)
            LOG.error(f"Failed to create table {object_name} at {str(self)}. Error: {error}.")
            return 0
        else:
            LOG.info(f"Successfully created table {object_name} at {str(self)}.")
            return 1
