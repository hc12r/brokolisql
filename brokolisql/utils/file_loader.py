import pandas as pd
import json
import xml.etree.ElementTree as ET
from brokolisql.services import normalizer
from brokolisql.services import type_inference
import os
from brokolisql.exceptions import (
    FileNotFound,
    FileFormatNotSupported,
    FileLoadError,
    FileParsingError,
)


def load_file(filepath, format='auto'):
    """
    Load a file into a pandas DataFrame, normalize column names,
    and infer column types.
    
    Args:
        filepath (str): Path to the input file.
        format (str): Format of the file. If 'auto', infer from extension.
        
    Returns:
        tuple: (DataFrame, column types dict)
        
    Raises:
        ValueError: If the file cannot be read or format is unsupported.
    """
    if not os.path.exists(filepath):
        raise FileNotFound(filepath)

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
            raise FileFormatNotSupported(ext)
    
    try:
        if format == 'csv':
            df = pd.read_csv(filepath)
        elif format == 'excel':
            df = pd.read_excel(filepath)
        elif format == 'json':
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.json_normalize(data)
        elif format == 'xml':
            try:
                tree = ET.parse(filepath)
                root = tree.getroot()
                elements = [{elem.tag: elem.text for elem in child} for child in root]
                df = pd.DataFrame(elements)
            except ET.ParseError as e:
                raise FileParsingError(filepath)
        else:
            raise FileFormatNotSupported()
    except Exception as e:
        raise FileLoadError(filepath, e)

    try:
        df = normalizer.normalize_column_names(df)
        column_types = type_inference.infer_column_types(df)
    except Exception as e:
        raise FileLoadError(filepath, e)
    
    return df, column_types