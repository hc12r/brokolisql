from tqdm import tqdm

def generate_sql(df, table_name, dialect, batch_size=1):
    """
    Generate SQL INSERT statements with support for batch inserts
    and SQL dialects.
    
    Args:
        df (DataFrame): The dataframe to generate SQL for
        table_name (str): Name of the table to insert into
        dialect (SQLDialect): Dialect object for the target database
        batch_size (int): Number of rows per INSERT statement
        
    Returns:
        list: A list of SQL statements as strings
    """
    sql_statements = []
    total_rows = len(df)
    
    # For single-row inserts
    if batch_size <= 1:
        for _, row in tqdm(df.iterrows(), total=total_rows, desc="Generating SQL"):
            cols = list(df.columns)
            values = [row[col] for col in cols]
            sql = dialect.create_insert_statement(table_name, cols, values)
            sql_statements.append(sql)
    
    # For batch inserts
    else:
        for i in tqdm(range(0, total_rows, batch_size), desc="Generating SQL batches"):
            batch = df.iloc[i:i+batch_size]
            if len(batch) == 0:
                continue
                
            cols = list(df.columns)
            value_groups = []
            
            for _, row in batch.iterrows():
                values = [dialect.format_value(row[col]) for col in cols]
                value_group = f"({', '.join(values)})"
                value_groups.append(value_group)
                
            col_str = ', '.join([dialect.format_column_name(col) for col in cols])
            values_str = ',\n  '.join(value_groups)
            
            sql = f"INSERT INTO {table_name} ({col_str}) VALUES\n  {values_str};"
            sql_statements.append(sql)
            
    return sql_statements
