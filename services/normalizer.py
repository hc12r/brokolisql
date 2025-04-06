def normalize_column_names(df):
    """
    Normalize the column names by replacing spaces with underscores
    and converting to uppercase.
    """
    df.columns = [col.replace(' ', '_').upper() for col in df.columns]
    return df
