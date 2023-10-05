import logging
import logger

import sqlalchemy
from sqlalchemy.engine import URL
import pyodbc
import pandas as pd
import keyring as kr

class SQLHandler:
    def __init__(
            self, 
            environment: str = None,
            server: str = None,
            database: str = None,
            uid: str = None,
            pwd: str = None,
            driver: str = "{SQL Server}",
            trusted_connection: str = 'yes',
            encrypt: str = 'yes',
            trust_server_certificate: str = 'yes',
            connection_timeout: int = 60
    ):
        if environment is None and (server is None or database is None):
            logging.critical("Not enough information given to start SQLHandler.")
            return
        self.params = locals()
        self.params.pop('self')
        self.params.pop('environment')
        self.params['connection_timeout'] = str(connection_timeout)
        if environment is not None:
            for param in self.params.keys():
                value = kr.get_password(environment, param)
                if value is not None:
                    self.params[param] = value
        self.connected = False
        self.connect_to_mssql()
        return
        
    def connect_to_mssql(self):
        if self.connected:
            logging.warning("Connection already open.")
            return
        logging.info(f"Attempting to connect to [{self.params['server']}].[{self.params['database']}].")
        connstring = ""
        for (param, value) in self.params.items():
            if value is not None:
                connstring += param + "=" + value + ";"
        connurl = URL.create("mssql+pyodbc", query = {"odbc_connect": connstring})
        try:
            self.engine = sqlalchemy.create_engine(connurl)
            self.conn = self.engine.connect()
        except:
            logging.error(f"Failed to connect to [{self.params['server']}].[{self.params['database']}].")
        else:
            self.connected = True
            logging.info(f"Successfully connected to [{self.params['server']}].[{self.params['database']}].")
        return
    
    def execute_select(self, query: str):
        if not self.connected:
            logging.warning("No open connection.")
            return
        logging.info(f"Reading query against [{self.params['server']}].[{self.params['database']}].")
        return pd.read_sql_query(query, self.conn)
    
    def execute_query(self, query: str):
        if not self.connected:
            logging.warning("No open connection.")
            return
        logging.info(f"Running script against [{self.params['server']}].[{self.params['database']}].")
        self.conn.execute(query)
    
    def close_connection(self):
        if not self.connected:
            logging.warning("No open connection.")
            return
        self.conn.close()
        self.engine.dispose()
        self.connected = False
        logging.info(f"Closed connection to [{self.params['server']}].[{self.params['database']}].")
        return

logging.info('SQLHandler class configured.')
