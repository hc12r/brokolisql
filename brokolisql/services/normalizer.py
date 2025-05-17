def normalize_column_names(df):
    """
    Normalize the column names by replacing spaces with underscores,
    removing special characters, and converting to uppercase.
    """
    import re
    df.columns = [re.sub(r'[^\w]', '_', col).replace(' ', '_').upper() for col in df.columns]
    return df