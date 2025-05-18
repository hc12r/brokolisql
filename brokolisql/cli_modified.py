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
    
    # Create the main parser
    parser = argparse.ArgumentParser(description="BrokoliSQL - Convert various data sources to SQL INSERT statements")
    parser.add_argument('--version', action='version', version=f"BrokoliSQL {__version__}")
    
    # Create subparsers for different input types
    subparsers = parser.add_subparsers(dest='source_type', help='Type of data source')
    
    # File input subparser
    file_parser = subparsers.add_parser('file', help='Use a file as input source')
    file_parser.add_argument('--input', required=True, help='Path to the input file (CSV, Excel, JSON, XML)')
    file_parser.add_argument('--format', default='auto', help='Force input format (csv, excel, json, xml)')
    
    # Database input subparser
    db_parser = subparsers.add_parser('db', help='Use a database as input source')
    db_parser.add_argument('--dialect', required=True, help='Database dialect (mysql, postgres, sqlite, oracle, sqlserver)')
    db_parser.add_argument('--host', help='Database host')
    db_parser.add_argument('--port', type=int, help='Database port')
    db_parser.add_argument('--user', help='Database user')
    db_parser.add_argument('--password', help='Database password')
    db_parser.add_argument('--database', required=True, help='Database name')
    db_parser.add_argument('--driver', help='Database driver (for SQL Server)')
    db_parser.add_argument('--table', required=True, help='Table to read from')
    db_parser.add_argument('--query', help='Custom SQL query (overrides --table)')
    db_parser.add_argument('--limit', type=int, help='Limit the number of rows to read')
    db_parser.add_argument('--where', help='WHERE clause for filtering rows')
    
    # API input subparser
    api_parser = subparsers.add_parser('api', help='Use a REST API as input source')
    api_parser.add_argument('--url', required=True, help='API endpoint URL')
    api_parser.add_argument('--method', default='GET', choices=['GET', 'POST'], help='HTTP method')
    api_parser.add_argument('--headers', help='HTTP headers in JSON format')
    api_parser.add_argument('--auth', help='Authentication credentials in format username:password')
    api_parser.add_argument('--data', help='Request payload in JSON format')
    api_parser.add_argument('--path', help='JSON path to extract data from response')
    
    # Common arguments for all input types
    for p in [file_parser, db_parser, api_parser]:
        p.add_argument('--output', required=True, help='Path to the output SQL file')
        p.add_argument('--table', required=True, help='Name of the SQL table to insert into')
        p.add_argument('--sql-dialect', default='generic', help='SQL dialect for output (mysql, postgres, sqlite, oracle, sqlserver)')
        p.add_argument('--create-table', action='store_true', help='Generate CREATE TABLE statement')
        p.add_argument('--batch-size', type=int, default=1, help='Number of rows per INSERT statement')
        p.add_argument('--transform', help='Path to transformation config file')
        p.add_argument('--debug', action='store_true', help='Show full tracebacks for debugging')
    
    args = parser.parse_args()
    
    # If no source type is specified, show help and exit
    if args.source_type is None:
        parser.print_help()
        sys.exit(1)
        
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
    # Load data from the appropriate source
    if args.source_type == 'file':
        data, column_types = load_file(args.input, format=args.format)
        print(f"Loaded {len(data)} rows from '{args.input}' with columns: {list(data.columns)}")
    
    elif args.source_type == 'db':
        from brokolisql.connectors.database import DatabaseConnector
        from brokolisql.services.type_inference import infer_column_types
        
        # Create database connector
        connector = DatabaseConnector(
            dialect=args.dialect,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            driver=args.driver
        )
        
        # Connect and fetch data
        connector.connect()
        if args.query:
            data = connector.read_query(args.query)
        else:
            data = connector.read_table(args.table, limit=args.limit, where=args.where)
            
        # Normalize column names and infer types
        from brokolisql.services.normalizer import normalize_column_names
        data = normalize_column_names(data)
        column_types = infer_column_types(data)
        
        print(f"Loaded {len(data)} rows from database with columns: {list(data.columns)}")
    
    elif args.source_type == 'api':
        from brokolisql.connectors.api import APIConnector
        from brokolisql.services.type_inference import infer_column_types
        
        # Create API connector
        connector = APIConnector(
            url=args.url,
            method=args.method,
            headers=args.headers,
            auth=args.auth,
            data=args.data,
            path=args.path
        )
        
        # Fetch data
        data = connector.fetch_data()
        
        # Normalize column names and infer types
        from brokolisql.services.normalizer import normalize_column_names
        data = normalize_column_names(data)
        column_types = infer_column_types(data)
        
        print(f"Loaded {len(data)} rows from API with columns: {list(data.columns)}")
    
    # Apply transformations if specified
    if args.transform:
        from brokolisql.transformers.transform_engine import apply_transformations
        data = apply_transformations(data, args.transform)
    
    # Get the SQL dialect for output
    dialect = get_dialect(args.sql_dialect if hasattr(args, 'sql_dialect') else args.dialect)
    
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