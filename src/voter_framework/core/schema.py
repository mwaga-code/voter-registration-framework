"""
Schema definitions for voter registration data.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class VoterSchema:
    """Schema definition for normalized voter registration data."""
    
    # Required fields
    first_name: str = 'first_name'
    last_name: str = 'last_name'
    birth_date: str = 'birth_date'
    registration_date: str = 'registration_date'
    address: str = 'address'
    city: str = 'city'
    state: str = 'state'
    zip_code: str = 'zip_code'
    
    # Optional fields
    middle_name: str = 'middle_name'
    suffix: str = 'suffix'
    gender: str = 'gender'
    party: str = 'party'
    precinct: str = 'precinct'
    county: str = 'county'
    congressional_district: str = 'congressional_district'
    legislative_district: str = 'legislative_district'
    
    @property
    def required_fields(self) -> List[str]:
        """Get list of required field names."""
        return [
            self.first_name,
            self.last_name,
            self.birth_date,
            self.registration_date,
            self.address,
            self.city,
            self.state,
            self.zip_code
        ]
    
    @property
    def optional_fields(self) -> List[str]:
        """Get list of optional field names."""
        return [
            self.middle_name,
            self.suffix,
            self.gender,
            self.party,
            self.precinct,
            self.county,
            self.congressional_district,
            self.legislative_district
        ]
    
    @property
    def all_fields(self) -> List[str]:
        """Get list of all field names."""
        return self.required_fields + self.optional_fields
    
    def get_field_type(self, field_name: str) -> str:
        """
        Get the expected data type for a field.
        
        Args:
            field_name: Name of the field
            
        Returns:
            Expected data type as string
        """
        date_fields = [self.birth_date, self.registration_date]
        numeric_fields = [self.zip_code, self.precinct, self.congressional_district, self.legislative_district]
        
        if field_name in date_fields:
            return 'date'
        elif field_name in numeric_fields:
            return 'numeric'
        else:
            return 'string'
    
    def compare_schemas(self, schema1_fields: List[str], schema2_fields: List[str]) -> Dict:
        """
        Compare two schemas and identify differences.
        
        Args:
            schema1_fields: List of fields from first schema
            schema2_fields: List of fields from second schema
            
        Returns:
            Dictionary containing schema comparison results
        """
        comparison = {
            'missing_in_schema1': [],
            'missing_in_schema2': [],
            'common_fields': [],
            'schema1_only': [],
            'schema2_only': []
        }
        
        # Find missing required fields
        for field in self.required_fields:
            if field not in schema1_fields:
                comparison['missing_in_schema1'].append(field)
            if field not in schema2_fields:
                comparison['missing_in_schema2'].append(field)
        
        # Compare all fields
        for field in set(schema1_fields + schema2_fields):
            if field in schema1_fields and field in schema2_fields:
                comparison['common_fields'].append(field)
            elif field in schema1_fields:
                comparison['schema1_only'].append(field)
            else:
                comparison['schema2_only'].append(field)
        
        return comparison 