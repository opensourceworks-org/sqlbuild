import json
import argparse
import sys

def escape_string(value):
    """
    Escapes single quotes in a string by replacing them with two single quotes.
    """
    return value.replace("'", "''")

def format_value(value):
    """
    Formats the value based on its type for SQL insertion.
    - Strings are enclosed in single quotes with proper escaping.
    - None is converted to NULL.
    - Other data types are converted to their string representation.
    """
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        return f"'{escape_string(value)}'"
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    else:
        return str(value)

def generate_upsert_statements(data, table_name, unique_key, columns=None):
    """
    Generates PostgreSQL UPSERT (INSERT ... ON CONFLICT) SQL statements from JSON data.

    Parameters:
    - data (list of dict): The JSON data as a list of dictionaries.
    - table_name (str): The target PostgreSQL table name.
    - unique_key (str): The column name(s) to use for the ON CONFLICT clause.
                        For multiple columns, provide a comma-separated string.
    - columns (list of str, optional): Specific columns to include in the UPSERT.
                                       If None, all keys from the first JSON object are used.

    Returns:
    - List of UPSERT SQL statements as strings.
    """
    if not data:
        return []

    # Determine columns to use
    if columns is None:
        columns = list(data[0].keys())

    # Split unique_key into list if it's a comma-separated string
    unique_keys = [key.strip() for key in unique_key.split(',')]

    # Validate that unique_keys are in columns
    for key in unique_keys:
        if key not in columns:
            raise ValueError(f"Unique key '{key}' is not among the selected columns.")

    upsert_statements = []

    for row in data:
        # Ensure all selected columns are present in the row
        row_data = {col: row.get(col) for col in columns}

        # Prepare column names and values
        col_names = ', '.join(row_data.keys())
        col_values = ', '.join([format_value(row_data[col]) for col in row_data])

        # Prepare SET clause for DO UPDATE
        set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in row_data if col not in unique_keys])

        # Construct the UPSERT statement
        upsert = f"INSERT INTO {table_name} ({col_names}) VALUES ({col_values}) ON CONFLICT ({', '.join(unique_keys)}) DO UPDATE SET {set_clause};"

        upsert_statements.append(upsert)

    return upsert_statements

def main():
    parser = argparse.ArgumentParser(description="Generate PostgreSQL UPSERT statements from a JSON file.")
    parser.add_argument('json_file', help='Path to the input JSON file.')
    parser.add_argument('-o', '--output', help='Path to the output SQL file. If not provided, outputs to stdout.')
    parser.add_argument('-t', '--table', required=True, help='Target PostgreSQL table name.')
    parser.add_argument('-u', '--unique', required=True, help='Unique key column(s) for ON CONFLICT. For multiple columns, separate them with commas.')
    parser.add_argument('-c', '--columns', help='Comma-separated list of columns to include. If not provided, all columns from JSON are used.')

    args = parser.parse_args()

    # Load JSON data
    try:
        with open(args.json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("JSON data must be an array of objects.", file=sys.stderr)
        sys.exit(1)

    # Parse columns if provided
    columns = None
    if args.columns:
        columns = [col.strip() for col in args.columns.split(',')]

    try:
        upserts = generate_upsert_statements(data, args.table, args.unique, columns)
    except ValueError as ve:
        print(f"Error generating UPSERT statements: {ve}", file=sys.stderr)
        sys.exit(1)

    # Output the UPSERT statements
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                for stmt in upserts:
                    f.write(stmt + '\n')
            print(f"UPSERT statements have been written to '{args.output}'.")
        except Exception as e:
            print(f"Error writing to output file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        for stmt in upserts:
            print(stmt)

if __name__ == "__main__":
    main()
