import pandas as pd
import json
import csv
import os
from tqdm import tqdm
from brokolisql.services.normalizer import normalize_column_names
from brokolisql.services.type_inference import infer_column_types
from brokolisql.exceptions.base import FileFormatNotSupported, FileLoadError

class DataStreamer:
    """
    Stream and process large data files incrementally to reduce memory usage.
    """
    
    def __init__(self, filepath, format='auto', chunk_size=10000):
        """
        Initialize the data streamer.
        
        Args:
            filepath (str): Path to input file
            format (str): Format of the file, or 'auto' to infer from extension
            chunk_size (int): Number of rows to process at once
        """
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.format = self._determine_format(filepath, format)
        self.column_types = None
        self.rows_processed = 0
        self.schema_inferred = False
    
    def _determine_format(self, filepath, format_hint):
        """Determine the file format."""
        if format_hint != 'auto':
            return format_hint
        
        ext = os.path.splitext(filepath)[-1].lower()
        if ext == '.csv':
            return 'csv'
        elif ext in ['.xls', '.xlsx']:
            return 'excel'
        elif ext == '.json':
            return 'json'
        elif ext in ['.xml', '.html']:
            return 'xml'
        else:
            raise FileFormatNotSupported(ext)
    
    def _create_iterator(self):
        """Create an appropriate iterator for the file format."""
        if self.format == 'csv':
            return pd.read_csv(self.filepath, chunksize=self.chunk_size)
        elif self.format == 'excel':
            # Excel doesn't support proper streaming, but we can emulate it
            # by reading chunks of rows at a time
            try:
                import openpyxl
                wb = openpyxl.load_workbook(self.filepath, read_only=True)
                sheet = wb.active
                total_rows = sheet.max_row
                
                for start_row in range(2, total_rows + 1, self.chunk_size):
                    end_row = min(start_row + self.chunk_size - 1, total_rows)
                    chunk = pd.read_excel(
                        self.filepath, 
                        skiprows=range(1, start_row),
                        nrows=end_row - start_row + 1
                    )
                    if not chunk.empty:
                        yield chunk
            except ImportError:
                # Fall back to non-streaming for Excel if openpyxl not available
                yield pd.read_excel(self.filepath)
        elif self.format == 'json':
            # For JSON, we need to determine if it's an array or object structure
            with open(self.filepath, 'r') as f:
                char = f.read(1).strip()
            
            if char == '[':  # Array of objects - can stream
                import ijson
                with open(self.filepath, 'rb') as f:
                    # Buffer records until we have enough for a chunk
                    records = []
                    for item in ijson.items(f, 'item'):
                        records.append(item)
                        if len(records) >= self.chunk_size:
                            df = pd.DataFrame(records)
                            yield df
                            records = []
                    
                    # Yield any remaining records
                    if records:
                        df = pd.DataFrame(records)
                        yield df
            else:  # Single object - can't stream effectively
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    yield pd.DataFrame([data])
                else:
                    yield pd.DataFrame(data)
        else:
            # XML doesn't support streaming well, so just read it all at once
            raise ValueError(f"Streaming not supported for {self.format} format")
    
    def process_file(self, dialect=None, table_name=None, output_writer=None, transform_config=None):
        """
        Stream through the file, processing each chunk incrementally.
        
        Args:
            dialect: SQL dialect object for generating statements
            table_name (str): Name of the target SQL table
            output_writer: Writer object for outputting SQL statements
            transform_config (str): Path to transformation config file
            
        Returns:
            dict: Column types inferred from the data
        """
        from brokolisql.transformers.transform_engine import apply_transformations
        
        try:
            iterator = self._create_iterator()
            first_chunk = True
            total_rows = 0
            
            # If transform config is specified, load it
            transform_func = None
            if transform_config:
                transform_func = lambda df: apply_transformations(df, transform_config)
            
            for chunk in tqdm(iterator, desc="Processing data in chunks"):
                # Normalize column names on first chunk
                if first_chunk:
                    chunk = normalize_column_names(chunk)
                    if not self.schema_inferred:
                        self.column_types = infer_column_types(chunk)
                        self.schema_inferred = True
                        
                        # If we need to create table, write the CREATE statement
                        if dialect and table_name and output_writer:
                            create_stmt = dialect.create_table_statement(table_name, self.column_types)
                            output_writer.write_statement(create_stmt)
                
                # Apply transformations if needed
                if transform_func:
                    chunk = transform_func(chunk)
                
                # Generate and write INSERT statements if needed
                if dialect and table_name and output_writer:
                    from brokolisql.services.sql_generator import generate_sql_for_chunk
                    statements = generate_sql_for_chunk(chunk, table_name, dialect)
                    for stmt in statements:
                        output_writer.write_statement(stmt)
                
                # Update counters
                total_rows += len(chunk)
                first_chunk = False
            
            print(f"Processed {total_rows} rows from '{self.filepath}'")
            return self.column_types
            
        except Exception as e:
            raise FileLoadError(self.filepath, e)
