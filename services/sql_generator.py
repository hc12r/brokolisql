import pandas as pd

def generate_sql(df, table_name, column_types):
    sql_statements = []
    columns = ', '.join([f'"{col}"' for col in df.columns])
    for _, row in df.iterrows():
        values = ', '.join([format_value(val) for val in row])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
        sql_statements.append(sql)
    return sql_statements

def format_value(val):
    if pd.isna(val):
        return 'NULL'
    if isinstance(val, str):
        return f"'{val.replace("'", "''")}'"
    return str(val)
