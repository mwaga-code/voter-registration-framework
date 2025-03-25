"""
Data validation utilities for voter registration data.
"""

from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
from .schema import VoterSchema

class DataValidator:
    """Validator for voter registration data."""
    
    def __init__(self):
        """Initialize the validator with the voter schema."""
        self.schema = VoterSchema()
    
    def validate(self, data: pd.DataFrame) -> Dict:
        """
        Validate voter registration data.
        
        Args:
            data: DataFrame containing voter data
            
        Returns:
            Dictionary containing validation results
        """
        validation = {
            'is_valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check required fields
        missing_fields = [field for field in self.schema.required_fields 
                         if field not in data.columns]
        if missing_fields:
            validation['is_valid'] = False
            validation['errors'].append(f"Missing required fields: {missing_fields}")
        
        # Validate data types
        for field in data.columns:
            field_type = self.schema.get_field_type(field)
            if field_type == 'date':
                try:
                    pd.to_datetime(data[field])
                except Exception as e:
                    validation['warnings'].append(
                        f"Invalid date format in {field}: {str(e)}"
                    )
            elif field_type == 'numeric':
                try:
                    pd.to_numeric(data[field], errors='raise')
                except Exception as e:
                    validation['warnings'].append(
                        f"Invalid numeric format in {field}: {str(e)}"
                    )
        
        # Validate ZIP codes
        if 'zip_code' in data.columns:
            invalid_zips = data[~data['zip_code'].str.match(r'^\d{5}(-\d{4})?$', na=False)]
            if not invalid_zips.empty:
                validation['warnings'].append(
                    f"Found {len(invalid_zips)} invalid ZIP codes"
                )
        
        # Validate state codes
        if 'state' in data.columns:
            invalid_states = data[~data['state'].str.match(r'^[A-Z]{2}$', na=False)]
            if not invalid_states.empty:
                validation['warnings'].append(
                    f"Found {len(invalid_states)} invalid state codes"
                )
        
        # Validate names
        for field in ['first_name', 'last_name']:
            if field in data.columns:
                empty_names = data[data[field].isna() | (data[field] == '')]
                if not empty_names.empty:
                    validation['warnings'].append(
                        f"Found {len(empty_names)} empty {field}s"
                    )
        
        return validation
    
    def validate_address(self, address_data: pd.DataFrame) -> Dict:
        """
        Validate address data.
        
        Args:
            address_data: DataFrame containing address data
            
        Returns:
            Dictionary containing address validation results
        """
        validation = {
            'is_valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check required address fields
        required_fields = ['address', 'city', 'state', 'zip_code']
        missing_fields = [field for field in required_fields 
                         if field not in address_data.columns]
        if missing_fields:
            validation['is_valid'] = False
            validation['errors'].append(f"Missing required address fields: {missing_fields}")
        
        # Validate ZIP codes
        if 'zip_code' in address_data.columns:
            invalid_zips = address_data[~address_data['zip_code'].str.match(r'^\d{5}(-\d{4})?$', na=False)]
            if not invalid_zips.empty:
                validation['warnings'].append(
                    f"Found {len(invalid_zips)} invalid ZIP codes"
                )
        
        # Validate state codes
        if 'state' in address_data.columns:
            invalid_states = address_data[~address_data['state'].str.match(r'^[A-Z]{2}$', na=False)]
            if not invalid_states.empty:
                validation['warnings'].append(
                    f"Found {len(invalid_states)} invalid state codes"
                )
        
        return validation 