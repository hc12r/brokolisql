import argparse
from brokolisql.utils.file_loader import load_file
from brokolisql.services.sql_generator import generate_sql
from brokolisql.output.output_writer import write_output
from brokolisql.dialects import get_dialect
from brokolisql.exceptions import BrokoliSQLException
import sys
import importlib.resources
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = brokoli_version = version("brokolisql") 
except PackageNotFoundError:
    print("BrokoliSQL version not found. Make sure the package is installed correctly.")
    __version__ = "unknown"

def print_banner():
    try:
        from brokolisql import assets
        with importlib.resources.open_text(assets, 'banner.txt') as f:
            banner = f.read()
        print(banner)
    except (FileNotFoundError, ImportError):
        print("BrokoliSQL is a Python-based command-line tool designed to facilitate the conversion of structured data files—such as CSV, Excel, JSON, and XML—into SQL INSERT statements.")

def main():
    print_banner()
    parser = argparse.ArgumentParser(description="BrokoliSQL - Convert CSV/Excel to SQL INSERT statements")
    parser.add_argument('--version', action='version', version=f"BrokoliSQL {__version__}")
    parser.add_argument('--input', required=True, help='Path to the input CSV or Excel file')
    parser.add_argument('--output', required=True, help='Path to the output SQL file')
    parser.add_argument('--table', required=True, help='Name of the SQL table to insert into')
    parser.add_argument('--dialect', default='generic', help='SQL dialect (mysql, postgres, sqlite, oracle, sqlserver)')
    parser.add_argument('--create-table', action='store_true', help='Generate CREATE TABLE statement')
    parser.add_argument('--batch-size', type=int, default=1, help='Number of INSERT statements per batch')
    parser.add_argument('--format', default='auto', help='Force input format (csv, excel, json, xml)')
    parser.add_argument('--transform', help='Path to transformation config file')
    parser.add_argument('--debug', action='store_true', help='Show full tracebacks for debugging')
    
    args = parser.parse_args()
    
    # Load and transform data

    try:
        run(args)
    except BrokoliSQLException as e:
        print(f"\n{e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print("\nUnexpected error:", str(e))
        if args.debug:
            import traceback
            traceback.print_exc()
        else:
            print("Run with --debug for more information.")
        sys.exit(1)

def run(args):
    # Load and transform data
    data, column_types = load_file(args.input, format=args.format)
    print(f"Loaded {len(data)} rows from '{args.input}' with columns: {list(data.columns)}")

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
