import pandas as pd
import json
import xml.etree.ElementTree as ET
from brokolisql.services import normalizer
from brokolisql.services import type_inference
import os

def load_file(filepath, format='auto'):
    """
    Load a file into a pandas DataFrame, normalize column names,
    and infer column types.
    
    Args:
        filepath (str): Path to the input file.
        format (str): Format of the file. If 'auto', infer from extension.
        
    Returns:
        tuple: (DataFrame with normalized column names, inferred column types dictionary)
        
    Raises:
        ValueError: If file extension is not supported.
    """
    if format == 'auto':
        ext = os.path.splitext(filepath)[-1].lower()
        if ext == '.csv':
            format = 'csv'
        elif ext in ['.xls', '.xlsx']:
            format = 'excel'
        elif ext == '.json':
            format = 'json'
        elif ext in ['.xml', '.html']:
            format = 'xml'
        else:
            raise ValueError(f"Unsupported file extension: {ext}. Please specify format manually.")
    
    # Load based on format
    if format == 'csv':
        df = pd.read_csv(filepath)
    elif format == 'excel':
        df = pd.read_excel(filepath)
    elif format == 'json':
        with open(filepath, 'r') as f:
            data = json.load(f)
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            # Handle nested JSON structures
            df = pd.json_normalize(data)
    elif format == 'xml':
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # Find repeating elements (likely records)
        elements = []
        for child in root:
            elements.append({elem.tag: elem.text for elem in child})
        df = pd.DataFrame(elements)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    # Normalize column names and infer types
    df = normalizer.normalize_column_names(df)
    column_types = type_inference.infer_column_types(df)
    
    return df, column_types