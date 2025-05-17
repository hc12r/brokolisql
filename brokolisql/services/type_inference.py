def infer_column_types(df):
    """
    Infer SQL column types from pandas data types with more advanced detection.
    
    Args:
        df (DataFrame): The dataframe to infer column types from.
        
    Returns:
        dict: A dictionary with column names as keys and SQL types as values.
    """
    import numpy as np
    import pandas as pd
    import re
    
    sql_types = {}
    
    for col in df.columns:
        # Skip completely empty columns
        if df[col].isna().all():
            sql_types[col] = 'VARCHAR(255)'
            continue
        
        dtype = df[col].dtype
        non_null_sample = df[col].dropna()
        
        # No data to analyze
        if len(non_null_sample) == 0:
            sql_types[col] = 'VARCHAR(255)'
            continue
            
        # Check integer types
        if np.issubdtype(dtype, np.integer):
            # Check value ranges to determine appropriate integer type
            min_val = non_null_sample.min()
            max_val = non_null_sample.max()
            
            if min_val >= -128 and max_val <= 127:
                sql_types[col] = 'TINYINT'
            elif min_val >= -32768 and max_val <= 32767:
                sql_types[col] = 'SMALLINT'
            elif min_val >= -2147483648 and max_val <= 2147483647:
                sql_types[col] = 'INTEGER'
            else:
                sql_types[col] = 'BIGINT'
        
        # Check floating point types
        elif np.issubdtype(dtype, np.floating):
            # Check precision needed
            max_decimals = non_null_sample.apply(lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0).max()
            if max_decimals <= 6:
                sql_types[col] = 'FLOAT'
            else:
                sql_types[col] = 'DOUBLE'
        
        # Check date and time
        elif np.issubdtype(dtype, np.datetime64):
            if (non_null_sample.dt.time != pd.Timestamp('00:00:00').time()).any():
                sql_types[col] = 'TIMESTAMP'
            else:
                sql_types[col] = 'DATE'
        
        # Check string types
        elif dtype == 'object':
            # Sample data for string analysis
            sample = non_null_sample.sample(min(100, len(non_null_sample))).astype(str)
            
            # Check if it looks like a GUID/UUID
            uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
            if all(bool(uuid_pattern.match(x)) for x in sample):
                sql_types[col] = 'CHAR(36)'
                continue
                
            # Check if all values are True/False (boolean)
            if set(sample.str.lower()) <= {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}:
                sql_types[col] = 'BOOLEAN'
                continue
                
            # Calculate max length for VARCHAR
            max_length = non_null_sample.astype(str).str.len().max()
            
            # If very long text, use TEXT type
            if max_length > 255:
                sql_types[col] = 'TEXT'
            else:
                # Add some buffer to max length
                sql_types[col] = f'VARCHAR({min(max_length + 10, 255)})'
        
        # Default fallback
        else:
            sql_types[col] = 'VARCHAR(255)'
    
    return sql_types