import pandas as pd
import json
import os

def apply_transformations(df, config_path):
    """
    Apply transformations to the dataframe based on a config file.
    
    Args:
        df (DataFrame): The dataframe to transform
        config_path (str): Path to the transformation config file
        
    Returns:
        DataFrame: The transformed dataframe
    """
    
    env = {"pd": pd, "df": df}
    env.update(df)
    # Check file extension
    _, ext = os.path.splitext(config_path)
    
    # Load the config file
    if ext.lower() == '.json':
        with open(config_path, 'r') as f:
            config = json.load(f)
    elif ext.lower() in ('.yml', '.yaml'):
        import yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    else:
        raise ValueError(f"Unsupported config file extension: {ext}")
    
    # Apply each transformation in sequence
    for transform in config.get('transformations', []):
        transform_type = transform.get('type')
        
        if transform_type == 'rename_columns':
            rename_map = transform.get('mapping', {})
            df = df.rename(columns=rename_map)
        
        elif transform_type == 'drop_columns':
            columns = transform.get('columns', [])
            df = df.drop(columns=columns, errors='ignore')
        
        elif transform_type == 'filter_rows':
            condition = transform.get('condition', '')
            if condition:
                df = df.query(condition)
        
        elif transform_type == 'add_column':
            name = transform.get('name', '')
            expression = transform.get('expression', '')
            if name and expression:
                df[name] = eval(expression, env)
        
        elif transform_type == 'change_type':
            column = transform.get('column', '')
            target_type = transform.get('to_type', '')
            if column and target_type:
                df[column] = df[column].astype(target_type)
        
        elif transform_type == 'replace_values':
            column = transform.get('column', '')
            old_value = transform.get('old_value', None)
            new_value = transform.get('new_value', None)
            if column:
                if old_value is not None and new_value is not None:
                    df[column] = df[column].replace(old_value, new_value)
                elif transform.get('mapping'):
                     df.loc[:, column] = df[column].replace(transform.get('mapping'))
        
        elif transform_type == 'apply_function':
            column = transform.get('column', '')
            function_name = transform.get('function', '')
            
            if column and function_name:
                # Some common functions
                if function_name == 'upper':
                    df[column] = df[column].str.upper()
                elif function_name == 'lower':
                   df.loc[:, column] = df[column].str.lower()
                elif function_name == 'strip':
                    df[column] = df[column].str.strip()
                elif function_name == 'title':
                    df[column] = df[column].str.title()
                elif function_name == 'capitalize':
                    df[column] = df[column].str.capitalize()
        
        elif transform_type == 'sort':
            columns = transform.get('columns', [])
            ascending = transform.get('ascending', True)
            if columns:
                df = df.sort_values(by=columns, ascending=ascending)
        
        elif transform_type == 'aggregate':
            group_by = transform.get('group_by', [])
            aggregations = transform.get('aggregations', {})
            if group_by:
                df = df.groupby(group_by).agg(aggregations).reset_index()
        
    return df