from brokolisql.dialects.base import SQLDialect
import pandas as pd
from datetime import datetime, date

class SQLiteDialect(SQLDialect):
    """SQLite dialect implementation"""
    
    def format_column_name(self, name):
        """Format column name with SQLite double quotes"""
        return f'"{name}"'
    
    def format_value(self, val):
        """Format value for SQLite"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
        elif isinstance(val, (datetime, date)):
            return f"'{val}'"
        elif isinstance(val, bool):
            return '1' if val else '0'
        else:
            return str(val)
    
    def create_table_statement(self, table_name, column_types):
        """Create SQLite-specific CREATE TABLE statement"""
        columns_sql = []
        for col, sql_type in column_types.items():
            # Convert generic types to SQLite types (simplified types)
            if sql_type.startswith('VARCHAR') or sql_type == 'TEXT':
                sqlite_type = 'TEXT'
            elif sql_type in ('INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'):
                sqlite_type = 'INTEGER'
            elif sql_type in ('FLOAT', 'DOUBLE', 'REAL'):
                sqlite_type = 'REAL'
            elif sql_type in ('BOOLEAN'):
                sqlite_type = 'INTEGER'  # SQLite uses INTEGER for boolean
            elif sql_type in ('DATE', 'TIMESTAMP'):
                sqlite_type = 'TEXT'  # SQLite stores dates as text
            else:
                sqlite_type = 'TEXT'  # Default to TEXT for unknown types
                
            columns_sql.append(f"    {self.format_column_name(col)} {sqlite_type}")
        
        columns_def = ',\n'.join(columns_sql)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns_def}\n);"
