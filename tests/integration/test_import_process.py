#!/usr/bin/env python3
"""
Integration tests for the full import process.
"""

import unittest
import pandas as pd
import os
import sys
import sqlite3
import tempfile
import shutil
import time

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.voter_framework.cli.onboard_state import (
    create_state_config,
    analyze_columns,
    detect_file_format,
    save_config
)
from src.voter_framework.cli.import_to_sqlite import (
    create_table,
    import_data,
    get_table_name,
    read_data_file
)

class TestImportProcess(unittest.TestCase):
    """Integration tests for the full import process."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test configs
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = os.path.join(self.temp_dir, 'config')
        os.makedirs(self.config_dir, exist_ok=True)
        
        # Set up paths to test fixtures
        self.fixtures_dir = os.path.join(os.path.dirname(__file__), '../fixtures')
        self.wa_file_path = os.path.join(self.fixtures_dir, 'wa_test_data.csv')
        self.or_file_path = os.path.join(self.fixtures_dir, 'or_test_data.csv')
        self.ca_file_path = os.path.join(self.fixtures_dir, 'ca_test_data.csv')
        
        # Create a temporary SQLite database
        self.db_path = os.path.join(self.temp_dir, 'test_voter_data.db')
        self.conn = sqlite3.connect(self.db_path)

    def tearDown(self):
        """Clean up test fixtures."""
        # Close the database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)

    def test_wa_import_process(self):
        """Test the full import process for Washington format."""
        table_name = f"test_wa_import_{int(time.time())}"
        
        # Sample Washington data
        wa_data = {
            'statevoterid': ['WA123'],
            'fname': ['John'],
            'mname': ['A'],
            'lname': ['Smith'],
            'birthyear': ['1980'],
            'gender': ['M'],
            'regstnum': ['123'],
            'regstfrac': ['1/2'],
            'regstpredirection': ['N'],
            'regstname': ['Main'],
            'regsttype': ['St'],
            'regunittype': ['Apt'],
            'regstpostdirection': ['W'],
            'regstunitnum': ['4B'],
            'regcity': ['Seattle'],
            'regstate': ['WA'],
            'regzipcode': ['98101']
        }
        wa_df = pd.DataFrame(wa_data)
        
        # Expected mappings
        config = {
            'column_mappings': {
                'statevoterid': 'voter_id',
                'fname': 'first_name',
                'mname': 'middle_name',
                'lname': 'last_name',
                'birthyear': 'birth_year',
                'gender': 'gender',
                'regstnum': 'address_street_number',
                'regstfrac': 'address_street_fraction',
                'regstpredirection': 'address_street_pre_direction',
                'regstname': 'address_street_name',
                'regsttype': 'address_street_type',
                'regunittype': 'address_unit_type',
                'regstpostdirection': 'address_street_post_direction',
                'regstunitnum': 'address_unit_number',
                'regcity': 'city',
                'regstate': 'state',
                'regzipcode': 'zip_code'
            },
            'address_fields': {
                'address': {
                    'fields': [
                        'regstnum',
                        'regstfrac',
                        'regstpredirection',
                        'regstname',
                        'regsttype',
                        'regunittype',
                        'regstpostdirection',
                        'regstunitnum'
                    ],
                    'separator': ' '
                }
            }
        }
        
        # Import the data
        import_data(self.conn, table_name, wa_df, config['column_mappings'], config['address_fields'])
        
        # Verify the data was imported correctly
        cursor = self.conn.cursor()
        
        # Check first name
        cursor.execute(f"SELECT first_name FROM {table_name} LIMIT 1")
        self.assertEqual(cursor.fetchone()[0], 'John')
        
        # Check last name
        cursor.execute(f"SELECT last_name FROM {table_name} LIMIT 1")
        self.assertEqual(cursor.fetchone()[0], 'Smith')
        
        # Check address components
        cursor.execute(f"SELECT address_street_number, address_street_name, address_street_type FROM {table_name} LIMIT 1")
        row = cursor.fetchone()
        self.assertEqual(row[0], '123')
        self.assertEqual(row[1], 'Main')
        self.assertEqual(row[2], 'St')
    
    def test_or_import_process(self):
        """Test the full import process for Oregon format."""
        # First, detect format and read columns
        or_format, or_delimiter, or_columns = detect_file_format(self.or_file_path)
        
        # Read the data
        or_df = pd.read_csv(self.or_file_path, sep=or_delimiter, header=0, dtype=str)
        
        # Generate column mappings
        mappings = analyze_columns(or_df)
        
        # Create state config
        config = create_state_config('OR', self.or_file_path, mappings, or_df, or_columns)
        
        # Get a unique table name using timestamp
        table_name = f"test_or_import_{int(time.time())}"
        
        # Create the table
        create_table(self.conn, table_name)
        
        # Import the data
        import_data(self.conn, table_name, or_df, config['column_mappings'], config['address_fields'])
        
        # Verify the import
        cursor = self.conn.cursor()
        
        # Check total row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        self.assertEqual(row_count, 5, "Expected 5 rows in the Oregon test data")
    
    def test_ca_import_process(self):
        """Test the full import process for California format."""
        # First, detect format and read columns
        ca_format, ca_delimiter, ca_columns = detect_file_format(self.ca_file_path)
        
        # Read the data
        ca_df = pd.read_csv(self.ca_file_path, sep=ca_delimiter, header=0, dtype=str)
        
        # Generate column mappings
        mappings = analyze_columns(ca_df)
        
        # Create state config
        config = create_state_config('CA', self.ca_file_path, mappings, ca_df, ca_columns)
        
        # Get a unique table name using timestamp
        table_name = f"test_ca_import_{int(time.time())}"
        
        # Create the table
        create_table(self.conn, table_name)
        
        # Import the data
        import_data(self.conn, table_name, ca_df, config['column_mappings'], config['address_fields'])
        
        # Verify the import
        cursor = self.conn.cursor()
        
        # Check total row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        self.assertEqual(row_count, 5, "Expected 5 rows in the California test data")

if __name__ == '__main__':
    unittest.main() 