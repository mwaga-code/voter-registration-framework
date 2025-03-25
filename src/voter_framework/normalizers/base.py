"""
Base normalizer for voter registration data.
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime

class BaseDataNormalizer(ABC):
    """Abstract base class for normalizing voter registration data."""
    
    def __init__(self):
        """Initialize the normalizer."""
        self.required_fields = [
            'first_name',
            'last_name',
            'birth_date',
            'registration_date',
            'address',
            'city',
            'state',
            'zip_code'
        ]
    
    @abstractmethod
    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize voter registration data to a common format.
        
        Args:
            data: Raw voter registration data
            
        Returns:
            DataFrame containing normalized data
        """
        pass
    
    def validate_normalized_data(self, data: pd.DataFrame) -> Dict:
        """
        Validate normalized data against required fields and data types.
        
        Args:
            data: Normalized DataFrame to validate
            
        Returns:
            Dictionary containing validation results
        """
        validation = {
            'is_valid': True,
            'missing_fields': [],
            'invalid_types': [],
            'warnings': []
        }
        
        # Check required fields
        for field in self.required_fields:
            if field not in data.columns:
                validation['is_valid'] = False
                validation['missing_fields'].append(field)
        
        # Validate data types
        if 'birth_date' in data.columns:
            try:
                pd.to_datetime(data['birth_date'])
            except Exception:
                validation['is_valid'] = False
                validation['invalid_types'].append('birth_date')
        
        if 'registration_date' in data.columns:
            try:
                pd.to_datetime(data['registration_date'])
            except Exception:
                validation['is_valid'] = False
                validation['invalid_types'].append('registration_date')
        
        # Validate ZIP codes
        if 'zip_code' in data.columns:
            invalid_zips = data[~data['zip_code'].str.match(r'^\d{5}(-\d{4})?$', na=False)]
            if not invalid_zips.empty:
                validation['warnings'].append(f'Found {len(invalid_zips)} invalid ZIP codes')
        
        return validation
    
    def clean_name(self, name: str) -> str:
        """
        Clean and standardize a name field.
        
        Args:
            name: Raw name string
            
        Returns:
            Cleaned name string
        """
        if pd.isna(name):
            return ''
        
        # Convert to lowercase and remove extra whitespace
        name = ' '.join(name.lower().split())
        
        # Remove special characters except spaces and hyphens
        name = ''.join(c for c in name if c.isalnum() or c in [' ', '-'])
        
        return name
    
    def standardize_date(self, date_str: str) -> Optional[str]:
        """
        Standardize date string to ISO format.
        
        Args:
            date_str: Raw date string
            
        Returns:
            ISO format date string or None if invalid
        """
        if pd.isna(date_str):
            return None
        
        try:
            # Try various date formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d']:
                try:
                    date = datetime.strptime(str(date_str), fmt)
                    return date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return None
        except Exception:
            return None 