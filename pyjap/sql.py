from pyjap.logger import LOG

from pyjap.utilities import extract_param

import pandas as pd
import keyring as kr
import pyodbc
import os

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
        driver: str = None,
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
            LOG.critical("Not enough information given to start SQLHandler.")
            return
        self._connection_string = connection_string
        self._params = {
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
        if self._connection_string is not None:
            LOG.info("Reading parameters from connection string.")
            for param in [param for param, value in self._params.items() if value is None]:
                self._params[param] = extract_param(self._connection_string, param+'=', ';')
        elif environment is not None:
            LOG.info("Getting parameters from keyring.")
            for param in [param for param, value in self._params.items() if value is None]:
                value = kr.get_password(environment, param)
                if value is None:
                    LOG.warning(f'No value found for {param} in {environment} environment.')
                else:
                    self._params[param] = value

        if self._connection_string is None:
            self._connection_string = ""
            for param, value in [(param, value) for param, value in self._params.items() if value is not None]:
                self._connection_string += f"{param}={value};"

        self.description = ()
        return
    
    def __str__(self) -> str:
        """
        Returns a string representation of the connected server and database.

        Returns:
            str: A string in the format '[server].[database]'.
        """
        # To return detailed information, could set this to return the following format:
        # [SQL(server='server', database='database', etc...)]
        return f'[{self._params["server"]}].[{self._params["database"]}]'
    
    def __bool__(self) -> bool:
        return self.connected
    
    def _schema_table_to_object_name(self, schema: str, table: str) -> str:
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
    
    def connect_to_mssql(self, auto_commit: bool = False) -> pyodbc.Cursor|None:
        """
        Establishes a connection to the SQL Server.
        """
        if self.connected:
            LOG.error(f"Connection to {str(self)} already open.")
            return
        LOG.info(f'Attempting to connect to {self}...')
        try:
            self.conn = pyodbc.connect(self._connection_string, autocommit = auto_commit)
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
        Rolls back the current transaction.
        """
        if not self.connected:
            LOG.warning("No open connection to roll back.")
            return
        LOG.info(f"Rolling back transaction to {str(self)}.")
        self.conn.rollback()
        return
    
    def commit(self):
        """
        Commits the current transaction.
        """
        if not self.connected:
            LOG.warning("No open connection to commit.")
            return
        LOG.info(f"Committing transaction to {str(self)}.")
        self.conn.commit()
        return

    def close_connection(self, commit: bool = True):
        """
        Closes the open connection.

        Args:
            commit (bool, optional): Flag indicating whether to commit changes before closing. Default is True.
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
                LOG.error(f"Could not connect to {self}. {error}.")
                return
        LOG.info(f"Generating dataframe against {str(self)}.")
        results = self.execute_query(query, values, commit = False)
        # selection = pd.read_sql_query(query, con=self.conn)
        # selection = pd.DataFrame.from_records(results, columns = self.query_columns())
        return pd.DataFrame((tuple(row) for row in results), columns = self.query_columns())
    
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
                LOG.error(f"Could not connect to {self}. {error}.")
                return
        LOG.info(f"Running script against {str(self)}:\n{query}\nValues: {values}.")
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
    
    def query_columns(self, query: str = None, values = None):
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
        Inserts data into the specified table.

        Args:
        - schema (str): The schema of the target table.
        - table (str): The name of the target table.
        - cols (list, optional): The list of column names for the insertion.
        - values (list[list|tuple], optional): The list of values to insert.
        - df (pd.DataFrame, optional): A DataFrame containing data to insert.
        - fast_execute (bool, optional): Flag indicating whether to use fast executemany. Default is True.
        - auto_create_table (bool, optional): Flag indicating whether to auto-create the table if not exists. Default is True.
        - replace_table (bool, optional): Flag indicating whether to replace the existing table. Default is False.
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
                return None
        
        if auto_create_table:
            if not self.create_table(table, cols, schema = schema, replace = replace_table, commit = commit):
                LOG.error(f"Could not create table for insert.")
                return
        object_name = self._schema_table_to_object_name(schema, table)

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
        Creates a new table in the database.

        Args:
            table (str): The name of the table to create.
            columns (list, optional): List of column names.
            datatypes (list, optional): List of column data types.
            schema (str, optional): The schema of the table.
            replace (bool, optional): Flag indicating whether to replace the existing table. Default is False.

        Returns:
        - (bool): Success flag of method.
        """
        object_name = self._schema_table_to_object_name(schema, table)
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