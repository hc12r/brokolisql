import argparse
from utils.file_loader import load_file
from services.sql_generator import generate_sql
from output.output_writer import write_output

def print_banner():
    with open('banner.txt', 'r') as f:
        banner = f.read()
    print(f"{banner}")



def main():
    print_banner()
    parser = argparse.ArgumentParser(description="BrokoliSQL - Convert CSV/Excel to SQL INSERT statements")
    parser.add_argument('--input', required=True, help='Path to the input CSV or Excel file')
    parser.add_argument('--output', required=True, help='Path to the output SQL file')
    parser.add_argument('--table', required=True, help='Name of the SQL table to insert into')

    args = parser.parse_args()

    data,column_types = load_file(args.input)

    sql_statements = generate_sql(data, args.table, column_types)

    write_output(sql_statements, args.output)

if __name__ == '__main__':
    main()
