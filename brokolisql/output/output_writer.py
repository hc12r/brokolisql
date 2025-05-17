def write_output(sql_lines, output_path):
    """
    Write SQL statements to file with optional compression.
    
    Args:
        sql_lines (list): List of SQL statements
        output_path (str): Path to output file
    """
    import os
    import gzip
    
    # Check if output should be compressed
    _, ext = os.path.splitext(output_path)
    if ext.lower() == '.gz':
        with gzip.open(output_path, 'wt', encoding='utf-8') as f:
            for line in sql_lines:
                f.write(line + '\n')
    else:
        with open(output_path, 'w', encoding='utf-8') as f:
            for line in sql_lines:
                f.write(line + '\n')