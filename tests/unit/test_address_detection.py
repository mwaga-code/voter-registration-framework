#!/usr/bin/env python3
"""
Unit tests for address field detection logic.
"""

import unittest
import pandas as pd
from src.voter_framework.cli.onboard_state import analyze_address_fields


class TestAddressDetection(unittest.TestCase):
    """Tests for the address field detection functionality."""

    def test_wa_address_field_detection(self):
        """Test address field detection with Washington state format."""
        # Create sample data
        data = {
            'RegStNum': ['123'],
            'RegStFrac': ['1/2'],
            'RegStPreDirection': ['N'],
            'RegStName': ['Main'],
            'RegStType': ['St'],
            'RegUnitType': ['Apt'],
            'RegStPostDirection': ['W'],
            'RegStUnitNum': ['4B'],
            'RegCity': ['Seattle'],
            'RegState': ['WA'],
            'RegZipCode': ['98101']
        }
        df = pd.DataFrame(data)
        column_names = list(data.keys())
        
        # Test address field detection
        address_fields = analyze_address_fields(df, column_names)
        
        expected_fields = {
            'address': {
                'fields': [
                    'RegStNum',
                    'RegStFrac',
                    'RegStPreDirection',
                    'RegStName',
                    'RegStType',
                    'RegUnitType',
                    'RegStPostDirection',
                    'RegStUnitNum',
                    'RegCity',
                    'RegState',
                    'RegZipCode'
                ],
                'separator': ' '
            }
        }
        
        self.assertEqual(address_fields, expected_fields)
    
    def test_or_address_field_detection(self):
        """Test address field detection with Oregon state format."""
        # Create sample data
        data = {
            'RES_STREET_NAME': ['Main'],
            'RES_STREET_TYPE': ['St'],
            'RES_CITY': ['Portland'],
            'RES_STATE': ['OR'],
            'RES_ZIP': ['97201']
        }
        df = pd.DataFrame(data)
        column_names = list(data.keys())
        
        # Test address field detection
        address_fields = analyze_address_fields(df, column_names)
        
        # Verify all required fields are found
        for field in data.keys():
            self.assertIn(field, address_fields['address']['fields'], f"Required field {field} not found in address fields")
            
    def test_ca_address_field_detection(self):
        """Test address field detection with California state format."""
        # Create sample data with a single combined address field
        data = {
            'ADDRESS_FULL': ['123 Main St'],
            'CITY': ['Los Angeles'],
            'STATE': ['CA'],
            'ZIP': ['90001']
        }
        df = pd.DataFrame(data)
        column_names = list(data.keys())
        
        # Test address field detection
        address_fields = analyze_address_fields(df, column_names)
        
        # Verify the combined address field is detected
        self.assertEqual(address_fields['address']['fields'], ['ADDRESS_FULL'])
        
    def test_fallback_to_general_address_fields(self):
        """Test fallback mechanism when specific fields aren't found."""
        # Create sample data with generic address fields
        data = {
            'City': ['Seattle'],
            'State': ['WA'],
            'ZipCode': ['98101']
        }
        df = pd.DataFrame(data)
        column_names = list(data.keys())
        
        # Test address field detection
        address_fields = analyze_address_fields(df, column_names)
        
        # Verify the fields are found
        for field in data.keys():
            self.assertIn(field, address_fields['address']['fields'])


if __name__ == '__main__':
    unittest.main()