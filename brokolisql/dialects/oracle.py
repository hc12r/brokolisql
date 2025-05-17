from brokolisql.dialects.base import SQLDialect
import pandas as pd
from datetime import datetime, date

class OracleDialect(SQLDialect):
    """Oracle dialect implementation"""
    
    def format_column_name(self, name):
        """Format column name with Oracle double quotes"""
        return f'"{name}"'
    
    def format_value(self, val):
        """Format value for Oracle"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            return f"'{val.replace("'", "''")}'"
        elif isinstance(val, datetime):
            return f"TO_TIMESTAMP('{val.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        elif isinstance(val, date):
            return f"TO_DATE('{val.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')"
        elif isinstance(val, bool):
            return '1' if val else '0'
        else:
            return str(val)
    
    def create_table_statement(self, table_name, column_types):
        """Create Oracle-specific CREATE TABLE statement"""
        columns_sql = []
        for col, sql_type in column_types.items():
            # Convert generic types to Oracle types
            if sql_type.startswith('VARCHAR'):
                length = sql_type.split('(')[1].split(')')[0] if '(' in sql_type else '255'
                oracle_type = f"VARCHAR2({length})"
            elif sql_type == 'TEXT':
                oracle_type = "CLOB"
            elif sql_type == 'INTEGER':
                oracle_type = "NUMBER(10)"
            elif sql_type == 'BIGINT':
                oracle_type = "NUMBER(19)"
            elif sql_type == 'FLOAT':
                oracle_type = "FLOAT"
            elif sql_type == 'DOUBLE':
                oracle_type = "BINARY_DOUBLE"
            elif sql_type == 'BOOLEAN':
                oracle_type = "NUMBER(1)"
            elif sql_type == 'DATE':
                oracle_type = "DATE"
            elif sql_type == 'TIMESTAMP':
                oracle_type = "TIMESTAMP"
            else:
                oracle_type = sql_type
                
            columns_sql.append(f"    {self.format_column_name(col)} {oracle_type}")
        
        columns_def = ',\n'.join(columns_sql)
        return f"CREATE TABLE {table_name} (\n{columns_def}\n);"
    
    def create_insert_statement(self, table_name, columns, values):
        """Create Oracle INSERT statement"""
        cols = ', '.join([self.format_column_name(col) for col in columns])
        vals = ', '.join([self.format_value(val) for val in values])
        # Oracle doesn't use the semicolon traditionally
        return f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"