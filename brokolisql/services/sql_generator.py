import pandas as pd
from tqdm import tqdm

def generate_sql(df, table_name):
    sql_statements = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Generating SQL"):
        columns = ', '.join([f'"{col}"' for col in df.columns])
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