import pandas as pd
from brokolisql.services import normalizer as normalizer
from brokolisql.services import type_inference as type_inference
import os

def load_file(filepath):
    """
    Load a CSV or Excel file into a pandas DataFrame, normalize column names,
    and infer column types.
    Args:
        filepath (str): Path to the input file.
    Returns:
        DataFrame: Loaded data with normalized column names and inferred types.
    Raises:
        ValueError: If file extension is not supported.
    """
    ext = os.path.splitext(filepath)[-1].lower()
    if ext == '.csv':
        df = pd.read_csv(filepath)
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    
    # Normalize column names and infer types
    df = normalizer.normalize_column_names(df)
    column_types = type_inference.infer_column_types(df)
    return df, column_types
