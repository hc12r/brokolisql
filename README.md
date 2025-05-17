# **BrokoliSQL**

**Universal Data-to-SQL Converter**

**BrokoliSQL** is a Python-based command-line tool designed to facilitate the conversion of structured data files—such as CSV, Excel, JSON, and XML—into SQL `INSERT` statements. It solves common problems faced during data import, transformation, and database seeding by offering a flexible, extensible, and easy-to-use interface.

## **Key Features & Advantages**

* **Multi-format Support**: Accepts CSV, XLSX, JSON, and XML as input.
* **Database Dialect Flexibility**: Generates SQL for PostgreSQL, MySQL, SQLite, and others using the `--dialect` option.
* **Auto Table Creation**: Optionally generates a `CREATE TABLE` statement based on input data.
* **Batch Inserts**: Improves performance by writing multiple rows per `INSERT`.
* **Python-powered Transformations**: Allows column transformations using Python expressions in a JSON configuration.
* **Portable & Scriptable**: Lightweight CLI tool, easily integrated into data pipelines or automation scripts.
* **Robust Column Handling**: Automatically normalizes column names and infers SQL data types.
* **Open Source & Extensible**: Cleanly organized codebase, open to community contributions.

---

## **How It Works**

The execution process is straightforward:

1. Reads the input file (CSV, Excel, JSON, XML).
2. Normalizes column names (e.g., `Name Id` → `Name_ID`) for SQL compatibility.
3. Infers column types (`INTEGER`, `TEXT`, etc.).
4. Applies optional transformations defined via a Python-based JSON config.
5. Generates SQL `INSERT INTO` statements (and optionally `CREATE TABLE`).
6. Outputs the final SQL code to the specified file.

---

## **Installation**

Install from PyPI:

```bash
pip install brokolisql
```

Make sure you have Python 3 installed:

```bash
python --version
```

If you're working with the source code, install dependencies with:

```bash
pip install -r requirements.txt
```

---

## **Basic Usage**

```bash
brokolisql --input data.csv --output output.sql --table users
```

### Additional Usage Examples

Use a specific SQL dialect:

```bash
brokolisql --input data.csv --output output.sql --table users --dialect mysql
```

Generate a `CREATE TABLE` statement:

```bash
brokolisql --input data.csv --output output.sql --table users --create-table
```

Use batch inserts for better performance:

```bash
brokolisql --input data.csv --output output.sql --table users --batch-size 100
```

Specify input format explicitly:

```bash
brokolisql --input data.xml --output output.sql --table users --format xml
```

Apply Python-based transformations:

```bash
brokolisql --input data.csv --output output.sql --table users --transform transforms.json
```

### Example of `transforms.json`:

```json
{
  "transformations": [
    {
      "type": "rename_columns",
      "mapping": {
        "FIRST_NAME": "GIVEN_NAME",
        "LAST_NAME": "SURNAME",
        "PHONE_1": "PRIMARY_PHONE",
        "PHONE_2": "SECONDARY_PHONE"
      }
    },
    {
      "type": "add_column",
      "name": "FULL_NAME",
      "expression": "FIRST_NAME + ' ' + LAST_NAME"
    },
    {
      "type": "add_column",
      "name": "SUBSCRIPTION_AGE_DAYS",
      "expression": "(pd.Timestamp('today') - pd.to_datetime(df['SUBSCRIPTION_DATE'])).dt.days"
    },
    {
      "type": "filter_rows",
      "condition": "COUNTRY in ['USA', 'Canada', 'Norway', 'UK', 'Germany']"
    },
    {
      "type": "apply_function",
      "column": "EMAIL",
      "function": "lower"
    },
    {
      "type": "apply_function",
      "column": "WEBSITE",
      "function": "lower"
    },
    {
      "type": "replace_values",
      "column": "COUNTRY",
      "mapping": {
        "USA": "United States",
        "UK": "United Kingdom"
      }
    },
    {
      "type": "drop_columns",
      "columns": ["INDEX"]
    },
    {
      "type": "add_column",
      "name": "EMAIL_DOMAIN",
      "expression": "EMAIL.str.split('@').str[1]"
    },
    {
      "type": "sort",
      "columns": ["COUNTRY", "CITY", "GIVEN_NAME"],
      "ascending": true
    }
    ,
    {
      "type": "aggregate",
      "group_by": ["COUNTRY", "CITY"],
      "aggregations": {
        "CUSTOMER_COUNT": ["CUSTOMER_ID", "count"],
        "EARLIEST_JOIN": ["df['SUBSCRIPTION_DATE']", "min"],
        "LATEST_JOIN": ["df['SUBSCRIPTION_DATE']", "max"]
      }
    }
  ]
}

```

This enables flexible pre-processing logic during data conversion, such as cleaning strings, formatting dates, or extracting information.

---

## **Using the Script Directly**

If running directly from source:

```bash
PYTHONPATH=. python brokolisql/cli.py --input <path_to_input_file> --output <path_to_output_file> --table <table_name>
```

Example:

```bash
PYTHONPATH=. python brokolisql/cli.py --input data.csv --output commands.sql --table products
```

---

## **Project Structure**

```
├── cli.py
├── dialects
│   ├── base.py
│   ├── generic.py
│   ├── __init__.py
│   ├── mysql.py
│   ├── oracle.py
│   ├── postgres.py
│   ├── sqlite.py
│   └── sqlserver.py
├── examples
│   ├── customers-10000.csv
│   ├── customers-100.csv
│   ├── output.sql
│   └── transforms.json
├── output
│   └── output_writer.py
├── services
│   ├── normalizer.py
│   ├── sql_generator.py
│   └── type_inference.py
├── setup.py
├── transformers
│   ├── __init__.py
│   └── transform_engine.py
└── utils
    └── file_loader.py
```

---

## **Contributing**

We welcome contributions to improve BrokoliSQL. To contribute:

1. Fork the repository.

2. Clone it to your machine.

3. Create a feature branch:

   ```bash
   git checkout -b my-feature
   ```

4. Make your changes following **PEP 8** standards.

5. Add or update tests.

6. Commit and push your changes:

   ```bash
   git add .
   git commit -m "feat(minor): add X support"
   git push origin my-feature
   ```

7. Open a pull request describing your changes and why they are useful.

---

## **License**

BrokoliSQL is licensed under the **GNU GPL-3.0**. See the [LICENSE](LICENSE) file for more information.

---

## **Summary**

**BrokoliSQL** streamlines the process of converting structured data into clean, executable SQL. Whether you're migrating legacy data, seeding databases for development, or automating ingestion workflows, BrokoliSQL is a flexible and reliable tool that adapts to your needs. With support for Python-powered transformations and multiple database dialects, it brings power and simplicity to your data operations.

If you encounter any bugs, have suggestions, or would like to contribute, feel free to open an issue or submit a pull request.