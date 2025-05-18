import pandas as pd
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus
from brokolisql.exceptions.base import DatabaseConnectionError, DatabaseQueryError

class DatabaseConnector:
    """Class for connecting to and reading data from databases."""
    
    SUPPORTED_DIALECTS = {
        'mysql': 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}',
        'postgres': 'postgresql://{user}:{password}@{host}:{port}/{database}',
        'sqlite': 'sqlite:///{database}',
        'oracle': 'oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}',
        'sqlserver': 'mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}'
    }
    
    def __init__(self, dialect, host=None, port=None, user=None, password=None, database=None, driver=None):
        """Initialize database connector with connection parameters."""
        self.dialect = dialect.lower()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.driver = driver
        self.engine = None
        
        if self.dialect not in self.SUPPORTED_DIALECTS:
            raise ValueError(f"Unsupported database dialect: {dialect}. "
                             f"Supported dialects are: {', '.join(self.SUPPORTED_DIALECTS.keys())}")
    
    def connect(self):
        """Establish a connection to the database."""
        try:
            conn_str = self._build_connection_string()
            self.engine = create_engine(conn_str)
            # Test connection
            with self.engine.connect() as conn:
                pass
            return True
        except Exception as e:
            raise DatabaseConnectionError(
                f"Failed to connect to {self.dialect} database.", 
                f"Details: {str(e)}. Check your connection parameters and ensure required libraries are installed."
            )
    
    def _build_connection_string(self):
        """Build the connection string based on the dialect."""
        if self.dialect == 'sqlite':
            return self.SUPPORTED_DIALECTS[self.dialect].format(database=self.database)
        
        # Escape password for URL
        safe_password = quote_plus(self.password) if self.password else ""
        
        if self.dialect == 'sqlserver':
            return self.SUPPORTED_DIALECTS[self.dialect].format(
                user=self.user, 
                password=safe_password,
                host=self.host, 
                port=self.port or 1433, 
                database=self.database,
                driver=quote_plus(self.driver or "ODBC Driver 17 for SQL Server")
            )
        
        # Default format for other dialects
        return self.SUPPORTED_DIALECTS[self.dialect].format(
            user=self.user, 
            password=safe_password,
            host=self.host, 
            port=self.port or self._get_default_port(), 
            database=self.database
        )
    
    def _get_default_port(self):
        """Return default port number for the selected dialect."""
        default_ports = {
            'mysql': 3306,
            'postgres': 5432,
            'oracle': 1521,
            'sqlserver': 1433
        }
        return default_ports.get(self.dialect, 0)
    
    def list_tables(self):
        """List all tables in the connected database."""
        if not self.engine:
            self.connect()
        
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def read_table(self, table_name, limit=None, where=None):
        """
        Read data from a database table into a pandas DataFrame.
        
        Args:
            table_name (str): Name of the table to read
            limit (int, optional): Maximum number of rows to read
            where (str, optional): SQL WHERE clause
            
        Returns:
            pandas.DataFrame: DataFrame containing the table data
        """
        if not self.engine:
            self.connect()
        
        try:
            query = f"SELECT * FROM {table_name}"
            if where:
                query += f" WHERE {where}"
            if limit:
                query += f" LIMIT {limit}"
            
            return pd.read_sql(text(query), self.engine)
        except Exception as e:
            raise DatabaseQueryError(
                f"Failed to read from table '{table_name}'.",
                f"Details: {str(e)}. Check that the table exists and the query is valid."
            )
    
    def read_query(self, query):
        """
        Execute a custom SQL query and return results as a DataFrame.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pandas.DataFrame: DataFrame containing the query results
        """
        if not self.engine:
            self.connect()
        
        try:
            return pd.read_sql(text(query), self.engine)
        except Exception as e:
            raise DatabaseQueryError(
                f"Failed to execute query.",
                f"Details: {str(e)}. Check that your query syntax is correct."
            )
