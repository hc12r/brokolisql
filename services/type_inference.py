def infer_column_types(df):
    """
    Infer SQL column types from pandas data types.
    Args:
        df (DataFrame): The dataframe to infer column types from.
    Returns:
        dict: A dictionary with column names as keys and SQL types as values.
    """
    sql_types = {}
    for col, dtype in df.dtypes.items():
        if dtype == 'int64':
            sql_types[col] = 'INTEGER'
        elif dtype == 'float64':
            sql_types[col] = 'FLOAT'
        elif dtype == 'object':
            sql_types[col] = 'VARCHAR'
        elif dtype == 'datetime64[ns]':
            sql_types[col] = 'DATE'
        else:
            sql_types[col] = 'VARCHAR'
    return sql_types
