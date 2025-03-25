#!/usr/bin/env python3
"""
Unit tests for column mapping detection logic.
"""

import unittest
import pandas as pd
from src.voter_framework.cli.onboard_state import analyze_columns


class TestColumnMapping(unittest.TestCase):
    """Tests for the column mapping detection functionality."""

    def test_wa_column_mapping(self):
        """Test column mapping detection for Washington state format."""
        # Create sample DataFrame
        wa_df = pd.DataFrame({
            'StateVoterID': ['12345'],
            'FName': ['John'],
            'LName': ['Doe'],
            'MName': ['A'],
            'Birthyear': ['1980'],
            'Gender': ['M'],
            'RegStNum': ['123'],
            'RegStName': ['Main'],
            'RegStType': ['St'],
            'RegCity': ['Seattle'],
            'RegState': ['WA'],
            'RegZipCode': ['98101']
        })
        
        # Get mappings
        mappings = analyze_columns(wa_df)
        
        # Convert mappings to lowercase for case-insensitive comparison
        mappings_lower = {k.lower(): v for k, v in mappings.items()}
        
        # Check required fields
        required_cols = ['statevoterid', 'fname', 'lname', 'birthyear', 'gender']
        for col in required_cols:
            self.assertIn(col, mappings_lower, f"Column {col} not found in mappings")
        
        # Check address fields
        address_cols = ['regstnum', 'regstname', 'regsttype', 'regcity', 'regstate', 'regzipcode']
        found_address_field = False
        for col in address_cols:
            if col in mappings_lower and mappings_lower[col].startswith('address_'):
                found_address_field = True
                break
        
        self.assertTrue(found_address_field, "No address fields detected")
    
    def test_or_column_mapping(self):
        """Test column mapping detection for Oregon state format."""
        # Create sample DataFrame
        or_df = pd.DataFrame({
            'FirstName': ['Jane'],
            'LastName': ['Smith'],
            'MiddleName': ['B'],
            'BirthYear': ['1975'],
            'Sex': ['F'],
            'StreetNo': ['456'],
            'StreetName': ['Oak'],
            'StreetType': ['Ave'],
            'City': ['Portland'],
            'State': ['OR'],
            'Zip': ['97201']
        })
        
        # Get mappings
        mappings = analyze_columns(or_df)
        
        # Convert mappings to lowercase for case-insensitive comparison
        mappings_lower = {k.lower(): v for k, v in mappings.items()}
        
        # Check required fields
        required_cols = ['firstname', 'lastname', 'birthyear', 'sex']
        for col in required_cols:
            self.assertIn(col, mappings_lower, f"Column {col} not found in mappings")
        
        # Check address fields
        address_cols = ['streetno', 'streetname', 'streettype', 'city', 'state', 'zip']
        found_address_field = False
        for col in address_cols:
            if col in mappings_lower and mappings_lower[col].startswith('address_'):
                found_address_field = True
                break
        
        self.assertTrue(found_address_field, "No address fields detected")
    
    def test_ca_column_mapping(self):
        """Test column mapping detection for California state format."""
        # Create sample DataFrame
        ca_df = pd.DataFrame({
            'NAME_FIRST': ['Mary'],
            'NAME_LAST': ['Johnson'],
            'NAME_MIDDLE': ['C'],
            'BIRTH_YEAR': ['1990'],
            'GENDER': ['F'],
            'ADDRESS_FULL': ['789 Pine St'],
            'CITY': ['Los Angeles'],
            'STATE': ['CA'],
            'ZIP': ['90001']
        })
        
        # Get mappings
        mappings = analyze_columns(ca_df)
        
        # Convert mappings to lowercase for case-insensitive comparison
        mappings_lower = {k.lower(): v for k, v in mappings.items()}
        
        # Check required fields
        required_cols = ['name_first', 'name_last', 'birth_year', 'gender']
        for col in required_cols:
            self.assertIn(col, mappings_lower, f"Column {col} not found in mappings")
        
        # Check address fields
        address_cols = ['address_full', 'city', 'state', 'zip']
        found_address_field = False
        for col in address_cols:
            if col in mappings_lower:
                found_address_field = True
                break
        
        self.assertTrue(found_address_field, "No address fields detected")
    
    def test_column_mapping_general_patterns(self):
        """Test column mapping detection with various naming patterns."""
        # Create a DataFrame with different naming patterns for the same fields
        columns = [
            'id', 'voter_id', 'registration_id',
            'fname', 'lname', 'given_name', 'surname',
            'dob', 'birthdate', 'date_of_birth',
            'street_addr', 'city', 'postal', 'zipcode',
            'political_party', 'party_affiliation'
        ]
        
        data = {col: [f"test_{col}"] for col in columns}
        df = pd.DataFrame(data)
        
        # Call the function being tested
        mappings = analyze_columns(df)
        
        # Check that different patterns map to expected fields
        field_patterns = {
            'voter_id': ['id', 'voter_id', 'registration_id'],
            'first_name': ['fname', 'given_name'],
            'last_name': ['lname', 'surname'],
            'birth_year': ['dob', 'birthdate', 'date_of_birth'],
            'zip_code': ['postal', 'zipcode'],
            'party': ['political_party', 'party_affiliation']
        }
        
        for expected_field, patterns in field_patterns.items():
            # At least one pattern should map to the expected field
            pattern_mapped = False
            for pattern in patterns:
                if pattern in mappings and mappings[pattern] == expected_field:
                    pattern_mapped = True
                    break
            
            self.assertTrue(pattern_mapped, f"No pattern mapped to {expected_field}")


if __name__ == '__main__':
    unittest.main()