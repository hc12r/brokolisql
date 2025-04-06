def write_output(sql_lines, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        for line in sql_lines:
            f.write(line + '\n')