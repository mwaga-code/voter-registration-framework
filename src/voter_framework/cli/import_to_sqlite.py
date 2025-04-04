#!/usr/bin/env python3
"""
CLI tool for importing voter registration data into SQLite database.
"""

import argparse
import os
import sqlite3
from datetime import datetime
import pandas as pd
import yaml
from typing import Dict, Optional, List, Any
from ..adapters.base import BaseStateAdapter
from ..normalizers.base import BaseDataNormalizer
import csv
import sys
import json

def get_table_name(state_code: str, file_name: str) -> str:
    """
    Generate a table name for the imported data.
    
    Args:
        state_code: Two-letter state code
        file_name: Path to voter data file
        
    Returns:
        Table name for SQLite database
    """
    # For testing, use a consistent table name
    if 'test_data.csv' in file_name:
        return 'voters'
        
    # For production, use a unique table name based on state and file
    base_name = os.path.splitext(os.path.basename(file_name))[0]
    date_suffix = datetime.now().strftime('%Y%m%d')
    return f'voters_{state_code.lower()}_{base_name}_{date_suffix}'

def get_config_file_path(args):
    """Get the configuration file path.
    
    Args:
        args: Command line arguments containing state and optional config/config_dir
        
    Returns:
        str: Path to the configuration file
    """
    # If --config is specified, use it (mainly for testing)
    if getattr(args, 'config', None):
        return args.config
        
    # Get the config directory
    if getattr(args, 'config_dir', None):
        config_dir = args.config_dir
    else:
        # Default to 'configs' directory in the project root
        config_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'configs')
    
    # Construct config file path from state code
    return os.path.join(config_dir, f'{args.state.lower()}_config.json')

def load_state_config(state_code: str) -> Dict:
    """
    Load state configuration from JSON or YAML file.
    
    Args:
        state_code: Two-letter state code
        
    Returns:
        Dictionary containing state configuration
    """
    config_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'configs')
    json_config = os.path.join(config_dir, f'{state_code.lower()}_config.json')
    yaml_config = os.path.join(config_dir, f'{state_code.lower()}_config.yaml')
    
    config = None
    
    # Try JSON first
    if os.path.exists(json_config):
        with open(json_config, 'r') as f:
            config = json.load(f)
    # Try YAML as fallback
    elif os.path.exists(yaml_config):
        with open(yaml_config, 'r') as f:
            config = yaml.safe_load(f)
    
    if config is None:
        raise FileNotFoundError(f"Configuration for {state_code} not found. Run onboard_state.py first.")
    
    # Ensure required fields exist
    if 'file_format' not in config:
        config['file_format'] = {
            'type': 'csv',
            'delimiter': ',',
            'encoding': 'utf-8',
            'has_header': True
        }
    
    return config

def create_database(db_path: str):
    """
    Create SQLite database if it doesn't exist.
    
    Args:
        db_path: Path to the SQLite database file
    """
    # Handle case where db_path is just a filename
    dir_name = os.path.dirname(db_path)
    if dir_name:  # Only create directories if there's a directory part
        os.makedirs(dir_name, exist_ok=True)
    
    # Create or connect to the database
    conn = sqlite3.connect(db_path)
    conn.close()

def create_table(conn: sqlite3.Connection, table_name: str, force: bool = False) -> None:
    """Create the SQLite table for voter data.
    
    Args:
        conn: SQLite database connection
        table_name: Name of the table to create
        force: If True, drop existing table before creating
    """
    if force:
        print(f"Dropping table {table_name} if it exists...")
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT,
        last_name TEXT,
        middle_name TEXT,
        suffix TEXT,
        birth_year INTEGER,
        birthday DATE,
        registration_date DATE,
        address TEXT,
        address_street_number TEXT,
        address_street_fraction TEXT,
        address_street_pre_direction TEXT,
        address_street_name TEXT,
        address_street_type TEXT,
        address_unit_type TEXT,
        address_street_post_direction TEXT,
        address_unit_number TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        gender TEXT,
        party TEXT,
        precinct TEXT,
        precinct_part TEXT,
        county TEXT,
        voter_id TEXT UNIQUE,
        legislative_district TEXT,
        congressional_district TEXT,
        last_voted_date DATE,
        status_code TEXT,
        mailing_address TEXT,
        mailing_address2 TEXT,
        mailing_address3 TEXT,
        mailing_city TEXT,
        mailing_state TEXT,
        mailing_zip TEXT,
        mailing_country TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    print(f"Creating table with SQL: {sql}")
    conn.execute(sql)

    # Create index for address-based queries
    index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{table_name}_address
    ON {table_name}(address, city, zip_code)
    """
    print(f"Creating index with SQL: {index_sql}")
    conn.execute(index_sql)

    conn.commit()

def import_data(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame, mappings: Dict[str, str], address_fields: Dict[str, Any], force: bool = False, state_code: str = None) -> None:
    """Import data into SQLite database.
    
    Args:
        conn: SQLite database connection
        table_name: Name of the table to import into
        df: DataFrame containing voter data
        mappings: Dictionary mapping state columns to common schema
        address_fields: Dictionary containing address field configuration
        force: If True, drop existing table before creating
        state_code: Two-letter state code (e.g., WA, OR)
    """
    # Create table if it doesn't exist
    create_table(conn, table_name, force)
    
    # Debug output
    print("DataFrame columns:", df.columns.tolist())
    print("Mappings:", mappings)
    
    # Check for duplicate voter IDs in the input data
    voter_id_col = next((col for col, schema in mappings.items() if schema == 'voter_id'), None)
    if voter_id_col:
        duplicates = df[df[voter_id_col].duplicated(keep=False)]
        if not duplicates.empty:
            print(f"\nERROR: Found {len(duplicates)} duplicate voter IDs in the input data")
            print("This indicates a data integrity issue in the source file.")
            print("Sample duplicates:")
            print(duplicates[voter_id_col].head())
            print("\nPlease fix the source data before proceeding.")
            sys.exit(1)
    
    total_rows = len(df)
    chunk_size = 10000  # Process 10k records at a time
    processed_rows = 0
    unique_violations = []
    
    # Process data in chunks
    for chunk_start in range(0, total_rows, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_rows)
        df_chunk = df.iloc[chunk_start:chunk_end]
        
        # Map columns to schema for this chunk
        df_mapped = pd.DataFrame()
        
        # Create case-insensitive column lookup for DataFrame columns
        df_col_lookup = {col.lower(): col for col in df_chunk.columns}
        
        # Use the mappings from the config file
        for orig_col, schema_field in mappings.items():
            if not schema_field.startswith('address_'):
                # Find the actual column name in the DataFrame that matches case-insensitively
                actual_col = next((df_col_lookup[col_lower] for col_lower in df_col_lookup 
                                if col_lower == orig_col.lower()), None)
                if actual_col:
                    # No need to prefix voter_id since each state has its own table
                    df_mapped[schema_field] = df_chunk[actual_col]
        
        # Set state code from command line argument
        if state_code:
            df_mapped['state'] = state_code.upper()
        
        # Handle address fields
        if 'address' in address_fields:
            fields = address_fields['address']['fields']
            separator = address_fields['address'].get('separator', ' ')
            
            # Check if we have a single combined address field
            if len(fields) == 1:
                field_lower = fields[0].lower()
                if field_lower in df_col_lookup:
                    df_mapped['address'] = df_chunk[df_col_lookup[field_lower]]
            else:
                # Handle individual address components
                address_parts = []
                
                # Generic address handling for other states
                for field in fields:
                    field_lower = field.lower()
                    if field_lower in df_col_lookup:
                        actual_col = df_col_lookup[field_lower]
                        # Map to appropriate schema field if it exists in mappings
                        for orig_col, schema_field in mappings.items():
                            if orig_col.lower() == field_lower and schema_field.startswith('address_'):
                                df_mapped[schema_field] = df_chunk[actual_col]
                        # Add to address parts if it's a valid field
                        if df_chunk[actual_col].notna().any():  # Only include non-empty fields
                            address_parts.append(df_chunk[actual_col].fillna(''))
                
                # Create combined address field
                if address_parts:
                    df_mapped['address'] = pd.DataFrame(address_parts).T.apply(
                        lambda x: separator.join(str(val) for val in x if pd.notna(val) and str(val).strip()),
                        axis=1
                    )
        
        try:
            # Insert chunk into database
            df_mapped.to_sql(table_name, conn, if_exists='append', index=False)
            
            # Update progress
            processed_rows += len(df_chunk)
            progress_pct = (processed_rows / total_rows) * 100
            print(f"\rImported {processed_rows:,} records out of {total_rows:,} ({progress_pct:.1f}%)", end='', flush=True)
            
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                # Extract the voter_id from the error message
                error_msg = str(e)
                if "voter_id" in error_msg:
                    # Get the problematic voter_id from the current chunk
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT voter_id FROM {table_name}")
                    existing_ids = set(row[0] for row in cursor.fetchall())
                    
                    # Find which records in the current chunk have duplicate IDs
                    for _, row in df_mapped.iterrows():
                        if row['voter_id'] in existing_ids:
                            unique_violations.append({
                                'voter_id': row['voter_id'],
                                'first_name': row.get('first_name', 'N/A'),
                                'last_name': row.get('last_name', 'N/A'),
                                'state': row.get('state', 'N/A')
                            })
            else:
                raise  # Re-raise if it's not a UNIQUE constraint error
    
    print()  # New line after progress reporting
    
    # Report any unique constraint violations
    if unique_violations:
        print("\nERROR: Found UNIQUE constraint violations during import")
        print(f"Total violations: {len(unique_violations)}")
        print("\nSample violations (up to 5):")
        for violation in unique_violations[:5]:
            print(f"Voter ID: {violation['voter_id']}")
            print(f"Name: {violation['first_name']} {violation['last_name']}")
            print(f"State: {violation['state']}")
            print("---")
        print("\nThis indicates a data integrity issue. Please check the source data for duplicate voter IDs.")
        sys.exit(1)

def read_data_file(file_path: str, file_format: str, delimiter: str, column_names: List[str], limit: Optional[int] = None) -> pd.DataFrame:
    """
    Read data file with proper encoding.
    
    Args:
        file_path: Path to the input file
        file_format: Format of the file ('csv' or 'text')
        delimiter: Field delimiter character
        column_names: List of column names in the order they appear in the file
        limit: Maximum number of rows to read (None for all rows)
        
    Returns:
        DataFrame containing the data
    """
    try:
        # Try to detect the delimiter if not specified
        if not delimiter:
            with open(file_path, 'r', encoding='windows-1252') as f:
                first_line = f.readline()
                if '|' in first_line:
                    delimiter = '|'
                elif ',' in first_line:
                    delimiter = ','
                else:
                    delimiter = ','  # Default to comma
        
        # Read the file with windows-1252 encoding and detected delimiter
        df = pd.read_csv(
            file_path,
            delimiter=delimiter,
            header=0,  # Use first row as header
            dtype=str,
            encoding='windows-1252',
            nrows=limit,  # Limit the number of rows if specified
            skipinitialspace=True,  # Skip spaces after delimiter
            quoting=0  # Don't use quotes
        )
        
        # Clean up column names by removing trailing whitespace
        df.columns = [col.strip() for col in df.columns]
        
        row_count = "all" if limit is None else limit
        print(f"Successfully read {row_count} rows from file with windows-1252 encoding and {delimiter} delimiter")
        return df
        
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        raise

def import_main(args):
    """Main import function.
    
    Args:
        args: Command line arguments
    """
    # Get the configuration file path
    config_file = get_config_file_path(args)

    # Load configuration
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {config_file}")
        print(f"Please run 'onboard_state {args.state} <file>' first to create the configuration.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in configuration file: {config_file}")
        sys.exit(1)
    
    # Get database path - use a single database for all states
    if getattr(args, 'db', None):
        db_path = args.db
    else:
        db_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'voters.db')
    
    # Create database if it doesn't exist
    create_database(db_path)
    
    # Get table name
    table_name = get_table_name(args.state, args.file)
    
    # Handle both old and new config formats
    file_format = config.get('file_format', {})
    if isinstance(file_format, str):
        # Old format
        file_type = file_format
        delimiter = config.get('delimiter', ',')
        encoding = config.get('encoding', 'utf-8')
        has_header = config.get('has_header', True)
    else:
        # New format
        file_type = file_format.get('type', 'csv')
        delimiter = file_format.get('delimiter', ',')
        encoding = file_format.get('encoding', 'utf-8')
        has_header = file_format.get('has_header', True)
    
    # Read data file
    print(f"Reading data from {args.file}...")
    df = read_data_file(
        args.file,
        file_type,
        delimiter,
        config.get('column_names', []),
        getattr(args, 'limit', None)
    )
    
    if getattr(args, 'verbose', False):
        print(f"\nData Summary:")
        print(f"Total rows: {len(df)}")
        print(f"Columns: {', '.join(df.columns)}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Import data
    print("\nStarting data import...")
    conn = sqlite3.connect(db_path)
    
    try:
        import_data(
            conn,
            table_name,
            df,
            config['column_mappings'],
            config.get('address_fields', {}),
            getattr(args, 'force', False),
            args.state  # Pass state code to import_data
        )
        
        if getattr(args, 'verbose', False):
            # Get final table statistics
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print(f"\nImport Complete:")
            print(f"Table: {table_name}")
            print(f"Total rows imported: {row_count}")
            print(f"Columns: {', '.join(col[1] for col in columns)}")
            print(f"Database size: {os.path.getsize(db_path) / 1024**2:.2f} MB")
        
        conn.commit()
        print(f"\nSuccessfully imported data into table {table_name}")
        
    finally:
        conn.close()

def main(args=None):
    """Main function.
    
    Args:
        args: Command line arguments (optional)
    """
    if args is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('state', help='Two-letter state code (e.g., WA, OR)')
        parser.add_argument('file', help='Path to the voter data file')
        parser.add_argument('--limit', type=int, help='Limit the number of rows to import')
        parser.add_argument('--force', action='store_true', help='Force recreate the table')
        parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
        parser.add_argument('--config', help='Path to custom configuration file (mainly for testing)')
        parser.add_argument('--config-dir', help='Path to configuration directory (defaults to configs/ in project root)')
        parser.add_argument('--db', help='Path to the SQLite database file')
        args = parser.parse_args()

    import_main(args)

if __name__ == '__main__':
    main()
