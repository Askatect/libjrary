"""
# sql.py

Version: 3.1
Authors: JRA
Date: 2024-02-23

#### Explanation:
Contains the sqlhandler class for operating on SQL Server databases.

#### Requirements:
- pyjra.logger.LOG: For logging.
- pyjra.utilities.extract_param: For reading parameter values from connection strings.
- pandas: For DataFrames.
- keyring: For storing and retrieving of keys.
- pyodbc: To interface with the database.
- time.sleep: Pause between connection retries.

#### Artefacts:
- SQLHandler (class): Operates on SQL Server databases.

#### Usage:
>>> from pyjra.sql import SQLHandler

#### History:
- 3.1 JRA (2024-02-23): Tabular implementation bug fixes.
- 3.0 JRA (2024-02-19): Implemented Tabular and removed select_to_dataframe and query_columns.
- 2.0 JRA (2024-02-12): Revamped error handling.
- 1.1 JRA (2024-02-09): Added retry_wait.
- 1.0 JRA (2024-02-08): Initial version.
"""
from pyjra.logger import LOG

from pyjra.utilities import extract_param
from pyjra.utilities import Tabular

import pandas as pd
import keyring as kr
import pyodbc
from time import sleep

class SQLHandler:
    """
    ## SQLHandler
        
    Version: 3.1
    Authors: JRA
    Date: 2024-02-23

    #### Explanation:
    Operates on SQL Server databases.

    #### Artefacts:
    - __connection_string (str): Connection string.
    - __params (dict): Connection parameters.
    - connected (bool): If true, indicates a successful active connection.
    - conn (pyodbc.Connection): Connection object.
    - cursor (pyodbc.Cursor)
    - __init__ (func): Initialises the handler.
    - __str__ (func): Returns the server and database of the handler.
    - __bool__ (func): Returns True when the handler is successfully connected.
    - __schema_table_to_object_name (func): Standardises a given schema and object to a bracket wrapped, stop separated string.
    - connect_to_mssql (func): Establishes a connection to the SQL Server.
    - rollback (func): Rolls back the current transaction.
    - commit (func): Commits the current transaction.
    - close_connection (func): Closes the open connection.
    - execute_query (func): Executes a SQL query and returns output - if any - as a pyjra.utilities.Tabular.
    - insert (func): Inserts data into a specified table.
    - create_table (func): Creates a table in the database.

    #### Usage:
    >>> executor = SQLHandler(environment = 'dev')
    >>> executor.execute_query("SELECT 'value' AS [column]").to_dict(0)
    {'column': 'value'}

    #### Tasklist:
    - Add a execute query method that returns a dictionary representing the first row. Would be useful for a list of values or parameters, such as the weekly summary for func-personal.

    #### History:
    - 3.1 JRA (2024-02-23): Tabular implementation bug fixes.
    - 3.0 JRA (2024-02-19): Implemented Tabular and removed select_to_dataframe and query_columns.
    - 2.0 JRA (2024-02-12): Revamped error handling.
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

        #### Explanation:
        Initialises the handler.

        #### Parameters:
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

        #### Usage:
        >>> executor = SQLHandler(environment = 'dev')

        #### History:
        - 1.1 JRA (2024-02-09): Added retry_wait.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if connection_string is None and environment is None and (server is None or database is None):
            error = "Not enough information given to start SQLHandler."
            LOG.critical(error)
            raise ValueError(error)
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

        #### Explanation:
        Returns the server and database of the handler.

        #### Returns:
        - (str)

        #### Usage:
        >>> print(executor)
        "[server].[database]"

        #### History:
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

        #### Explanation:
        Returns True when the handler is successfully connected.

        #### Returns:
        - self.connected (bool)

        #### Usage:
        >>> bool(executor)

        #### History:
        - 1.0 JRA (2024-02-09): Initial version.
        """
        return self.connected
    
    def __schema_table_to_object_name(self, schema: str, table: str) -> str:
        """
        ### __schema_table_to_object_name
        
        Version: 1.1
        Authors: JRA
        Date: 2024-02-13

        #### Explanation:
        Standardises a given schema and object to a bracket wrapped, stop separated string.

        #### Parameters:
        - schema (str)
        - table (str)

        #### Returns:
        - (str)

        #### Usage:
        >>> executor.__schema_table_to_object_name('schema', 'table')
        '[schema].[table]'

        #### History:
        - 1.1 JRA (2024-02-13): Added support for inputs that are already wrapped.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if schema is None or schema == '':
            schema = ''
        elif not (schema.startswith('[') and schema.endswith(']')):
            schema = '[' + schema + '].'
        if not (table.startswith('[') and table.endswith(']')):
            table = '[' + table + ']'
        return f"{schema}{table}"
    
    def connect_to_mssql(self, auto_commit: bool = False, retry_wait: int = None) -> pyodbc.Cursor|None:
        """
        ### connect_to_mssql

        Version: 2.0
        Authors: JRA
        Date: 2024-02-12

        #### Explanation:
        Establishes a connection to the SQL Server.

        #### Parameters:
        - auto_commit (bool): If true, transactions are committed by default. Default is false.
        - retry_wait (int): If populated, connections to the database are retried once on failure after this number of seconds. Defaults to no retry.

        #### Returns:
        - self.cursor (pyodbc.Cursor)

        #### Usage:
        >>> executor.connect_to_mssql()
        <executor.cursor>

        #### History:
        - 2.0 JRA (2024-02-12): Revamped error handling.
        - 1.1 JRA (2024-02-09): Added retry_wait.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if self.connected:
            LOG.error(f"Connection to {str(self)} already open.")
            return
        LOG.info(f'Attempting to connect to {self}...')
        retry = True
        while True:
            try:
                self.conn = pyodbc.connect(self.__connection_string, autocommit = auto_commit)
            except pyodbc.OperationalError as e:
                LOG.error(f"A database operational error occurred while connecting to {self}. {e}")
                retry_wait = retry_wait or self.retry_wait
                if retry and retry_wait is not None:
                    retry = False
                    LOG.info(f"Waiting {retry_wait} seconds before retrying connection.")
                    sleep(retry_wait)
                else:
                    raise
            except pyodbc.InterfaceError as e:
                LOG.error(f"A database interface error occurred while connecting to {self}. {e}")
                raise
            except Exception as e:
                LOG.critical(f"Unexpected {type(e)} error occurred whilst connecting to {self}. {e}")
                raise
            else:
                self.cursor = self.conn.cursor()
                self.connected = True
                LOG.info(f"Successfully connected to {self}.")
                break
        return self.cursor
    
    def rollback(self):
        """
        ### rollback

        Version: 1.0
        Authors: JRA
        Date: 2024-02-09

        #### Explanation:
        Rolls back transactions.

        #### Usage:
        >>> executor.rollback()

        #### History:
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

        #### Explanation:
        Commits transactions.

        #### Usage:
        >>> executor.commit()

        #### History:
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

        #### Explanation:
        Closes the open connection.

        #### Requirements:
        - SQLHandler.commit
        - SQLHandler.rollback

        #### Parameters:
        - commit (bool): If true, transaction is committed. Defaults to true.

        #### Usage:
        >>> executor.close_connection()

        #### History:
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
    
    # def select_to_dataframe(self, query: str, values: tuple = None) -> pd.DataFrame:
    #     """
    #     ### select_to_dataframe

    #     Version: 1.2
    #     Authors: JRA
    #     Date: 2024-02-19

    #     #### Explanation:
    #     Executes a DQL script and returns the result as a DataFrame.

    #     #### Requirements:
    #     - SQLHandler.connect_to_mssql

    #     #### Parameters:
    #     - query (str): The query to run.
    #     - values (tuple): The values to substitute into the query. Defaults to None.

    #     #### Returns:
    #     - (pandas.DataFrame): Output of query.

    #     #### Usage:
    #     >>> executor.select_to_dataframe("SELECT 'value' AS [column]")
    #     <pandas.DataFrame>

    #     #### History:
    #     - 1.2 JRA (2024-02-19): Implemented Tabular.
    #     - 1.1 JRA (2024-02-12): Error handling moved to connect_to_mssql.
    #     - 1.0 JRA (2024-02-09): Initial version.
    #     """
    #     if not self.connected:
    #         self.connect_to_mssql(auto_commit = False)
    #     LOG.info(f"Generating dataframe against {str(self)}.")
    #     results = self.execute_query(query, values, commit = False)
    #     return results.to_dataframe()
    
    def execute_query(self, query: str, values: tuple = None, commit: bool = True, name: str = None) -> None|Tabular:
        """
        ### execute_query

        Version: 3.1
        Authors: JRA
        Date: 2024-02-23

        #### Explanation:
        Executes a SQL query.

        #### Requirements:
        - SQLHandler.connect_to_mssql
        - SQLHandler.close_connection

        #### Parameters:
        - query (str): The query to run.
        - values (tuple): The values to substitute into the query. Defaults to None.
        - commit (bool): If true, the query is committed.
        - name (str): The name to assign to the results.

        #### Returns:
        - selection (None|Tabular): The output selection of the query.

        #### Usage:
        >>> executor.execute_query("SELECT 'value' AS [column]")

        #### History:
        - 3.1 JRA (2024-02-23): Added support for queries with no returns.
        - 3.0 JRA (2024-02-19): Refactored to use Tabular.
        - 2.0 JRA (2024-02-12): Revamped error handling.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if not self.connected:
            self.connect_to_mssql(auto_commit = commit)
        LOG.info(f"Running script against {str(self)}:\n{query}\nValues: {values}.")

        try:
            if values is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, (values))
        except pyodbc.ProgrammingError as e:
            LOG.error(f"Failed to parse script on {self}. {e}")
            raise
        except Exception as e:
            LOG.critical(f"Unexpected {type(e)} error occurred whilst executing query on {self}. {e}")
            raise

        try:
            selection = self.cursor.fetchall()
        except pyodbc.ProgrammingError as e:
            LOG.warning(f"Could not retrieve query results. {e}")
            selection = None
        except Exception as e:
            LOG.critical(f"Unexpected {type(e)} error occurred whilst retrieving query results on {self}. {e}")
            raise

        try:
            columns = [col[0] for col in self.cursor.description]
            datatypes = [col[1] for col in self.cursor.description]
        except TypeError as e:
            LOG.warning(f"No query results to read metadata of.")
            columns = None
            datatypes = None
        except Exception as e:
            LOG.critical(f"Unexpected {type(e)} error occurred whilst retrieving query results on {self}. {e}")
            raise

        if selection is not None:
            selection = Tabular(
                data = [tuple(row) for row in selection], 
                columns = columns,
                datatypes = datatypes,
                name = name
            )

        self.close_connection(commit)
        return selection
    
    # def query_columns(self, query: str = None, values: tuple = None):
    #     """
    #     ### query_columns

    #     Version: 2.0
    #     Authors: JRA
    #     Date: 2024-02-12

    #     #### Explanation:
    #     Retrieves the columns of a query or the most recently executed query.

    #     #### Requirements:
    #     - SQLHandler.execute_query

    #     #### Parameters:
    #     - query (str): The query to execute if needed. Defaults to the previous query.
    #     - values (tuple): The values to substitute into the query. Defaults to previous query.

    #     #### Returns:
    #     - cols (list[str]): A list of columns of the output of the select query.

    #     #### Usage:
    #     >>> executor.query_columns()
    #     ['column']

    #     #### History:
    #     - 2.0 JRA (2024-02-09): Revamped error handling.
    #     - 1.0 JRA (2024-02-09): Initial version.
    #     """
    #     if query is not None:
    #         self.execute_query(query, values, commit = False)

    #     try:
    #         cols = [col[0] for col in self.description]
    #     except TypeError as e:
    #         LOG.warning(f"No query description available from {self}. {e}")
    #         raise
    #     except Exception as e:
    #         LOG.critical(f"Unexpected {type(e)} error occurred when reading cursor description from {self}. {e}")
    #         raise
    #     else:
    #         return cols

    def insert(
        self, 
        schema: str,  
        table: str,
        data: Tabular|pd.DataFrame|list[tuple],
        columns: list[str] = None, 
        prescript: str = None,
        postscript: str = None,
        fast_execute: bool = True,
        auto_create_table: bool = True,
        replace_table: bool = False,
        commit: bool = True
    ):
        """
        ### insert

        Version: 2.2
        Authors: JRA
        Date: 2024-02-23

        #### Explanation:
        Inserts data into a specified table.

        #### Requirements:
        - SQLHandler.connect_to_mssql
        - SQLHandler.create_table
        - SQLHandler.close_connection

        #### Parameters:
        - schema (str): The schema of the object to insert to.
        - table (str): The table to insert to.
        - data (Tabular|pandas.DataFrame|list[tuple]): The values to be inserted.
        - columns (list[str]): The columns to insert to. Defaults to all columns of existing table. Defaults to None.
        - prescript (str): A script to run prior to the insert. Defaults to None.
        - postcript (str): A script to run after the insert. Defaults to None.
        - fast_execute (bool): If true, fast execute is utilised. Failed inserts are retried without fast execute. Defaults to true.
        - auto_create_table (bool): If true, the table is created if it does not already exist. Defaults to true.
        - replace_table (bool): If true, the table is replaced if it already exists. Defaults to false.
        - commit (bool): If true, the insert is committed. Defaults to true.

        #### Usage:
        >>> executor.insert('schema', 'table', df)

        #### Tasklist:
        - Add functionality to retry inserts without fast_executemany - not sure which error warrants the retry.

        #### History:
        - 2.2 JRA (2024-02-23): Fixed an issue where `len(data.col_count)` was attempted.
        - 2.1 JRA (2024-02-19): Implemented Tabular.
        - 2.0 JRA (2024-02-09): Revamped error handling.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        if data is None:
            LOG.error("No values given to insert.")
            return
        elif isinstance(data, pd.DataFrame):
            data = Tabular(data = data)
        elif isinstance(data, list):
            data = Tabular(data = data, columns = columns)
        elif not isinstance(data, Tabular):
            error = f"Invalid datatype passed to `data` argument of `SQLHandler.insert`."
            LOG.error(error)
            raise ValueError(error)
        
        if not self.connected:
            self.connect_to_mssql(auto_commit = commit)
        
        if auto_create_table:
            if not self.create_table(table = table, columns = data.columns, schema = schema, replace = replace_table, commit = commit):
                LOG.error(f"Could not create table for insert.")
                return
        object_name = self.__schema_table_to_object_name(schema, table)

        self.connect_to_mssql(commit)

        if prescript is not None:
            LOG.info(f"Running prescript...")
            self.execute_query(prescript)

        LOG.info(f"Inserting into {object_name} on {self}...")
        self.cursor.fast_executemany = fast_execute
        cmd = f"INSERT INTO {object_name}{'([' + '], ['.join(data.columns) + '])' if len(data.columns) > 0 else ''} VALUES ({'?' + (data.col_count - 1)*', ?'})"
        try:
            self.cursor.executemany(cmd, data.data)
        except pyodbc.ProgrammingError as e:
            LOG.error(f"Failed to parse script on {self}. {e}")
            raise
        except Exception as e:
            LOG.critical(f"Unexpected {type(e)} error occurred whilst performing insert to {object_name} on {self}. {e}")
            raise
        LOG.info(f"Insert was successful!")
        
        if postscript is not None:
            LOG.info(f"Running postscript...")
            self.execute_query(postscript)
        self.close_connection(commit = commit)

        # try:
        #     LOG.info(f"Inserting into {object_name} at {str(self)}...")
        #     self.connect_to_mssql(commit)
        #     if prescript is not None:
        #         LOG.info(f"Running script against {str(self)}:\n{prescript}")
        #         self.cursor.execute(prescript)
        #     self.cursor.fast_executemany = fast_execute
        #     cmd = f"INSERT INTO {object_name}{'([' + '], ['.join(cols) + '])' if len(cols) > 0 else ''} VALUES ({'?' + (len(values[0]) - 1)*', ?'})"
        #     try:
        #         self.cursor.executemany(cmd, values)
        #     except Exception as e:
        #         if fast_execute:
        #             LOG.error(f"Insert failed. {error}.\nAttempting insert without fast_executemany...")
        #             self.cursor.fast_executemany = False
        #             self.cursor.executemany(cmd, values)
        #     if postscript is not None:
        #         LOG.info(f"Running script against {str(self)}:\n{postscript}")
        #         self.cursor.execute(postscript)
        # except Exception as e:
        #     self.close_connection(commit = False)
        #     LOG.error(f"Insert failed. Error: {error}.")
        # else:
        #     LOG.info(f"Insert complete!")
        #     self.close_connection(commit)
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

        Version: 2.2
        Authors: JRA
        Date: 2024-02-23

        #### Explanation: 
        Creates a table in the database.

        #### Requirements:
        - SQLHandler.__schema_table_to_object_name
        - SQLHandler.execute_query
        - SQLHandler.close_connection

        #### Parameters:
        - table (str): The name of the table.
        - columns (list[str]): The columns of the table.
        - datatypes (list[str]): The datatypes of the columns.
        - schema (str): The schema of the table. Defaults to None.
        - replace (bool): If true, if the table name already exists, then that table is dropped first. Defaults to False.
        - commit (bool): If true, the transaction is committed. Defaults to True.

        #### Returns:
        - (bool): True if the transaction passed without issues.

        #### Usage:
        >>> executor.create_table('table', ['column'], ['varchar(16)'])

        #### History:
        - 2.2 JRA (2024-02-23): Added column alias to `max_length` query.
        - 2.1 JRA (2024-02-19): Implemented Tabular.
        - 2.0 JRA (2024-02-12): Revamped error handling.
        - 1.0 JRA (2024-02-09): Initial version.
        """
        object_name = self.__schema_table_to_object_name(schema, table)
        LOG.info(f"Creating {object_name} on {self}...")

        if not replace:
            result = self.execute_query(
                query = "SELECT OBJECT_ID(?) AS [result]",
                values = (object_name),
                commit = False
            ).to_dict(0)['result']
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
            max_length = self.execute_query("SELECT CONVERT(int, [max_length]/2) AS [length] FROM sys.types WHERE [system_type_id] = 231", commit = False).to_dict(0)['length']
            datatypes.extend([f'nvarchar({max_length})']*(col_count - datatype_count))
        
        column_definition = ',\n\t'.join(f"[{col}] {datatype}" for col, datatype in zip(columns, datatypes))

        cmd += f"CREATE TABLE {object_name} (\n\t{column_definition}\n)"
        self.execute_query(cmd, commit = commit)
        LOG.info(f"Successfully created table {object_name} at {str(self)}.")
        return 1
