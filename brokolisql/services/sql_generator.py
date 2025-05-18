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


def generate_sql_for_chunk(df_chunk, table_name, dialect, batch_size=1):
    """
    Generate SQL INSERT statements for a DataFrame chunk.
    Similar to generate_sql but optimized for streaming.
    """
    # This function can be similar to the existing generate_sql function
    # but optimized for single-chunk processing
    sql_statements = []
    total_rows = len(df_chunk)
    
    # For single-row inserts
    if batch_size <= 1:
        for _, row in df_chunk.iterrows():
            cols = list(df_chunk.columns)
            values = [row[col] for col in cols]
            sql = dialect.create_insert_statement(table_name, cols, values)
            sql_statements.append(sql)
    
    # For batch inserts
    else:
        for i in range(0, total_rows, batch_size):
            batch = df_chunk.iloc[i:i+batch_size]
            if len(batch) == 0:
                continue
                
            cols = list(df_chunk.columns)
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