from brokolisql.dialects.base import SQLDialect
import pandas as pd
from datetime import datetime, date

class MySQLDialect(SQLDialect):
    """MySQL dialect implementation"""
    
    def format_column_name(self, name):
        """Format column name with MySQL backticks"""
        return f"`{name}`"
    
    def format_value(self, val):
        """Format value for MySQL"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            # MySQL escaping with backslash
            escaped = val.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')
            return f"'{escaped}'"
        elif isinstance(val, datetime):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(val, date):
            return f"'{val.strftime('%Y-%m-%d')}'"
        elif isinstance(val, bool):
            return '1' if val else '0'
        else:
            return str(val)
    
    def create_table_statement(self, table_name, column_types):
        """Create MySQL-specific CREATE TABLE statement"""
        columns_sql = []
        for col, sql_type in column_types.items():
            # Convert generic types to MySQL types if needed
            if sql_type == 'VARCHAR' or sql_type == 'VARCHAR(255)':
                mysql_type = 'VARCHAR(255)'
            elif sql_type == 'TEXT':
                mysql_type = 'TEXT'
            elif sql_type == 'INTEGER':
                mysql_type = 'INT'
            else:
                mysql_type = sql_type
                
            columns_sql.append(f"    {self.format_column_name(col)} {mysql_type}")
        
        columns_def = ',\n'.join(columns_sql)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns_def}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
