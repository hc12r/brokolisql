import pandas as pd
from datetime import datetime, date

class SQLDialect:
    """Base class for SQL dialects"""
    
    def format_column_name(self, name):
        """Format a column name according to the dialect's syntax"""
        return f'"{name}"'
    
    def format_value(self, val):
        """Format a value according to the dialect's syntax"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
        elif isinstance(val, (datetime, date)):
            return f"'{val}'"
        elif isinstance(val, bool):
            return str(val).upper()
        else:
            return str(val)
    
    def create_insert_statement(self, table_name, columns, values):
        """Create an INSERT statement"""
        cols = ', '.join([self.format_column_name(col) for col in columns])
        vals = ', '.join([self.format_value(val) for val in values])
        return f"INSERT INTO {table_name} ({cols}) VALUES ({vals});"
    
    def create_table_statement(self, table_name, column_types):
        """Create a CREATE TABLE statement"""
        columns_sql = []
        for col, sql_type in column_types.items():
            columns_sql.append(f"    {self.format_column_name(col)} {sql_type}")
        
        columns_def = ',\n'.join(columns_sql)
        return f"CREATE TABLE {table_name} (\n{columns_def}\n);"