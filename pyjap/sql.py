import logging

import pyodbc as sql
import sqlalchemy as db
from sqlalchemy.engine import URL
import keyring as kr

class SQLHandler:
    def __init__(self, environment):
        self.user = kr.get_password(environment, 'login')
        self.password = kr.get_password(environment, 'password')
        self.server = kr.get_password(environment, 'server')
        self.database = kr.get_password(environment, 'database')
        self.connected = False

    def connect_to_sqlserver(self, connstring = None):
        if self.connected:
            logging.warning(f"Connection to [{self.server}].[{self.database}] already established.")
            return
        if connstring is None:
            connstring = f"""
                Driver={'{ODBC Driver 17 for SQL Server}'};
                Server={self.server};
                Database={self.database};
                UID={self.user};
                PWD={self.password};
                Encrypt=yes;
                TrustServerCertificate=no;
                Connection Timeout=30;
            """
        logging.info(f'Connecting to [{self.server}].[{self.database}]...')
        try:
            self.conn = db.create_engine(URL.create("mssql+pyodbc", query = {"odbc_connect": connstring})) #sql.connect(connstring)
        except Exception:
            logging.critical(f"Exception occurred connecting to [{self.server}].[{self.database}]!", exc_info=True)
        else:
            self.connected = True
            self.connstring = connstring
            logging.info(f"Successfully connected to [{self.server}].[{self.database}].")
        return

    def execute_query(self, query, commit = True):
        if not self.connected:
            logging.error("Cannot execute query without connection.")
            return
        self.cursor = self.conn.cursor()
        self.cursor.execute(query)
        if commit:
            self.cursor.commit()
        return
    
    def close_connection(self):
        if self.connected:
            self.conn.close()
            self.connected = False
            logging.info(f"Connection to [{self.server}].[{self.database}] has been closed.")
        else:
            logging.warning("No open connection to close.")
        return
    
logging.info('SQLHandler class configured.')