from brokolisql.dialects.generic import GenericDialect
from brokolisql.dialects.mysql import MySQLDialect
from brokolisql.dialects.postgres import PostgresDialect
from brokolisql.dialects.sqlite import SQLiteDialect
from brokolisql.dialects.oracle import OracleDialect
from brokolisql.dialects.sqlserver import SQLServerDialect

def get_dialect(name):
    """
    Get a SQL dialect object by name.
    
    Args:
        name (str): Name of the dialect (case-insensitive)
        
    Returns:
        SQLDialect: A dialect object for the specified database
        
    Raises:
        ValueError: If dialect name is not supported
    """
    name = name.lower()
    
    if name == 'generic':
        return GenericDialect()
    elif name == 'mysql':
        return MySQLDialect()
    elif name in ('postgres', 'postgresql'):
        return PostgresDialect()
    elif name == 'sqlite':
        return SQLiteDialect()
    elif name == 'oracle':
        return OracleDialect()
    elif name in ('sqlserver', 'mssql'):
        return SQLServerDialect()
    else:
        raise ValueError(f"Unsupported dialect: {name}")