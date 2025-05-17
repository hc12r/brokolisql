from brokolisql.dialects.base import SQLDialect
import pandas as pd
from datetime import datetime, date

class SQLServerDialect(SQLDialect):
    """SQL Server dialect implementation"""
    
    def format_column_name(self, name):
        """Format column name with SQL Server brackets"""
        return f"[{name}]"
    
    def format_value(self, val):
        """Format value for SQL Server"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
        elif isinstance(val, datetime):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(val, date):
            return f"'{val.strftime('%Y-%m-%d')}'"
        elif isinstance(val, bool):
            return '1' if val else '0'
        else:
            return str(val)
    
    def create_table_statement(self, table_name, column_types):
        """Create SQL Server-specific CREATE TABLE statement"""
        columns_sql = []
        for col, sql_type in column_types.items():
            # Convert generic types to SQL Server types
            if sql_type.startswith('VARCHAR'):
                length = sql_type.split('(')[1].split(')')[0] if '(' in sql_type else '255'
                mssql_type = f"NVARCHAR({length})"
            elif sql_type == 'TEXT':
                mssql_type = "NVARCHAR(MAX)"
            elif sql_type == 'INTEGER':
                mssql_type = "INT"
            elif sql_type == 'TINYINT':
                mssql_type = "TINYINT"
            elif sql_type == 'SMALLINT':
                mssql_type = "SMALLINT"
            elif sql_type == 'BIGINT':
                mssql_type = "BIGINT"
            elif sql_type in ('FLOAT', 'DOUBLE'):
                mssql_type = "FLOAT"
            elif sql_type == 'BOOLEAN':
                mssql_type = "BIT"
            elif sql_type == 'DATE':
                mssql_type = "DATE"
            elif sql_type == 'TIMESTAMP':
                mssql_type = "DATETIME2"
            else:
                mssql_type = sql_type
                
            columns_sql.append(f"    {self.format_column_name(col)} {mssql_type}")
        
        columns_def = ',\n'.join(columns_sql)
        return f"CREATE TABLE {table_name} (\n{columns_def}\n);"