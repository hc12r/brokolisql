class StreamingOutputWriter:
    """Write SQL statements to file incrementally, without keeping them all in memory."""
    
    def __init__(self, output_path):
        """Initialize the streaming writer."""
        self.output_path = output_path
        self.is_gzipped = output_path.lower().endswith('.gz')
        self.file = self._open_file()
        
    def _open_file(self):
        """Open the output file for writing."""
        import os
        import gzip
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)
        
        if self.is_gzipped:
            return gzip.open(self.output_path, 'wt', encoding='utf-8')
        else:
            return open(self.output_path, 'w', encoding='utf-8')
    
    def write_statement(self, statement):
        """Write a single SQL statement to the file."""
        self.file.write(statement + '\n')
    
    def close(self):
        """Close the output file."""
        if self.file:
            self.file.flush()
            self.file.close()
            self.file = None
    
    def __del__(self):
        """Ensure file is closed when object is garbage collected."""
        self.close()