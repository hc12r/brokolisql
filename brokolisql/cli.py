import argparse
from brokolisql.utils.file_loader import load_file
from brokolisql.services.sql_generator import generate_sql
from brokolisql.output.output_writer import write_output
from brokolisql.dialects import get_dialect

def print_banner():
    try:
        with open('./banner.txt', 'r') as f:
            banner = f.read()
        print(f"{banner}")
    except FileNotFoundError:
        print("BrokoliSQL - CSV/Excel to SQL Converter")

def main():
    print_banner()
    parser = argparse.ArgumentParser(description="BrokoliSQL - Convert CSV/Excel to SQL INSERT statements")
    parser.add_argument('--input', required=True, help='Path to the input CSV or Excel file')
    parser.add_argument('--output', required=True, help='Path to the output SQL file')
    parser.add_argument('--table', required=True, help='Name of the SQL table to insert into')
    parser.add_argument('--dialect', default='generic', help='SQL dialect (mysql, postgres, sqlite, oracle, sqlserver)')
    parser.add_argument('--create-table', action='store_true', help='Generate CREATE TABLE statement')
    parser.add_argument('--batch-size', type=int, default=1, help='Number of INSERT statements per batch')
    parser.add_argument('--format', default='auto', help='Force input format (csv, excel, json, xml)')
    parser.add_argument('--transform', help='Path to transformation config file')
    
    args = parser.parse_args()
    
    # Load and transform data
    data, column_types = load_file(args.input, format=args.format)
    
    print(f"Loaded {len(data)} rows from {args.input} with {data.columns} columns.")
    
    # Apply transformations if specified
    if args.transform:
        from brokolisql.transformers.transform_engine import apply_transformations
        data = apply_transformations(data, args.transform)
    
    # Get the dialect
    dialect = get_dialect(args.dialect)
    
    # Generate SQL
    sql_statements = []
    if args.create_table:
        sql_statements.append(dialect.create_table_statement(args.table, column_types))
    
    sql_statements.extend(generate_sql(data, args.table, dialect, batch_size=args.batch_size))
    
    # Write output
    write_output(sql_statements, args.output)
    
    print(f"\nProcessed {len(data)} rows into {len(sql_statements)} SQL statements.")
    print("Done!\nexiting...")

if __name__ == '__main__':
    main()
