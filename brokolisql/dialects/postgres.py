from brokolisql.dialects.base import SQLDialect
import pandas as pd
from datetime import datetime, date

class PostgresDialect(SQLDialect):
    """PostgreSQL dialect implementation"""
    
    def format_column_name(self, name):
        """Format column name with PostgreSQL double quotes"""
        return f'"{name}"'
    
    def format_value(self, val):
        """Format value for PostgreSQL"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            # PostgreSQL standard escaping
            return f"'{val.replace("'", "''")}'"
        elif isinstance(val, datetime):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(val, date):
            return f"'{val.strftime('%Y-%m-%d')}'"
        elif isinstance(val, bool):
            return 'TRUE' if val else 'FALSE'
        else:
            return str(val)
    
    def create_table_statement(self, table_name, column_types):
        """Create PostgreSQL-specific CREATE TABLE statement"""
        columns_sql = []
        for col, sql_type in column_types.items():
            # Convert generic types to PostgreSQL types if needed
            if sql_type == 'INTEGER':
                pg_type = 'INTEGER'
            elif sql_type == 'BIGINT':
                pg_type = 'BIGINT'
            elif sql_type == 'FLOAT':
                pg_type = 'REAL'
            elif sql_type == 'DOUBLE':
                pg_type = 'DOUBLE PRECISION'
            elif sql_type == 'TEXT':
                pg_type = 'TEXT'
            elif sql_type.startswith('VARCHAR'):
                pg_type = sql_type
            elif sql_type == 'BOOLEAN':
                pg_type = 'BOOLEAN'
            elif sql_type == 'DATE':
                pg_type = 'DATE'
            elif sql_type == 'TIMESTAMP':
                pg_type = 'TIMESTAMP'
            else:
                pg_type = sql_type
                
            columns_sql.append(f"    {self.format_column_name(col)} {pg_type}")
        
        columns_def = ',\n'.join(columns_sql)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns_def}\n);"