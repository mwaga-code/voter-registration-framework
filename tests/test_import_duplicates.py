import os
import pytest
import subprocess
import sys
import json
from pathlib import Path

def test_duplicate_voter_ids():
    """Test that the import fails when duplicate voter IDs are found."""
    # Get the absolute path to the test data file
    test_data_path = Path(__file__).parent / 'data' / 'duplicate_test.csv'
    config_path = Path(__file__).parent / 'configs' / 'duplicate_test_config.json'
    
    # Create a temporary config file for the test
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    
    # Copy the test config to the configs directory
    with open(config_path, 'w') as f:
        json.dump({
            "file_format": {
                "type": "csv",
                "delimiter": ",",
                "encoding": "utf-8",
                "has_header": True
            },
            "column_mappings": {
                "voter_id": "voter_id",
                "first_name": "first_name",
                "last_name": "last_name",
                "address": "address",
                "city": "city",
                "state": "state",
                "zip_code": "zip_code"
            },
            "address_fields": {
                "address": {
                    "fields": ["address"],
                    "separator": " "
                }
            }
        }, f)
    
    try:
        # Run the import command
        cmd = [
            sys.executable,
            '-m',
            'voter_framework.cli.import_to_sqlite',
            '--config',
            str(config_path),
            'WA',
            str(test_data_path)
        ]
        
        # Run the command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Print output for debugging
        print("Command output:", result.stdout)
        print("Command error:", result.stderr)
        
        # Verify that the command failed
        assert result.returncode != 0, "Import should fail with duplicate voter IDs"
        
        # Verify the error message
        assert "ERROR: Found 2 duplicate voter IDs in the input data" in result.stdout
        assert "This indicates a data integrity issue in the source file" in result.stdout
        assert "12345" in result.stdout  # The duplicate voter ID should be shown
        
    finally:
        # Clean up
        if config_path.exists():
            config_path.unlink() 