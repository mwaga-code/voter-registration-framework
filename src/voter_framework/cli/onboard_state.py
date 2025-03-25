#!/usr/bin/env python3
"""
CLI tool for onboarding new state voter registration data.
"""

import argparse
import csv
import os
from datetime import datetime
import yaml
from typing import Dict, List, Optional, Any
import pandas as pd
from ..adapters.base import BaseStateAdapter
from ..normalizers.base import BaseDataNormalizer
import json
import sys

def detect_file_format(file_path: str) -> tuple[str, str, List[str]]:
    """
    Detect the format of the input file and read column names.
    
    Args:
        file_path: Path to the input file
        
    Returns:
        Tuple of (format, delimiter, column_names) where format is 'csv' or 'text', 
        delimiter is ',' or '|' or '\t', and column_names is a list of column names
    """
    # Try different encodings
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline().strip()
                # Split the line to get column names
                if ',' in first_line:
                    column_names = [col.strip() for col in first_line.split(',')]
                    return 'csv', ',', column_names
                elif '|' in first_line:
                    column_names = [col.strip() for col in first_line.split('|')]
                    return 'text', '|', column_names
                else:
                    column_names = [col.strip() for col in first_line.split('\t')]
                    return 'text', '\t', column_names
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not read file with any of the encodings: {encodings}")

def analyze_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Analyze DataFrame columns to suggest mappings to common schema.
    
    Args:
        df: DataFrame containing voter data
        
    Returns:
        Dictionary mapping state columns to common schema fields
    """
    # Common patterns in column names
    name_patterns = {
        'first_name': ['first', 'given', 'fname', 'firstname', 'name_first', 'fname'],
        'last_name': ['last', 'surname', 'lname', 'lastname', 'name_last', 'lname'],
        'middle_name': ['middle', 'mname', 'middlename', 'name_middle', 'mname'],
        'birth_year': ['birth', 'dob', 'birthdate', 'date_of_birth', 'birthyear', 'birth_year'],
        'birthday': ['birthday', 'day_of_birth'],
        'registration_date': ['registrationdate', 'regdate', 'registration_date', 'reg_date'],
        'city': ['city', 'town', 'regcity', 'municipality'],
        'state': ['state', 'regstate', 'province'],
        'zip_code': ['zip', 'postal', 'zipcode', 'regzipcode', 'postal_code'],
        'gender': ['sex', 'gender'],
        'party': ['party', 'political', 'affiliation', 'registration_party'],
        'precinct': ['precinct', 'district', 'precinctcode', 'precinct_id', 'voting_district'],
        'county': ['county', 'parish', 'countycode', 'county_name', 'jurisdiction'],
        'voter_id': ['voterid', 'voter_id', 'statevoterid', 'voter', 'votid', 'id', 'registration_id'],
        'legislative_district': ['legislativedistrict', 'legdistrict', 'leg_district', 'state_house'],
        'congressional_district': ['congressionaldistrict', 'congdistrict', 'cong_district', 'us_house'],
        'last_voted_date': ['lastvoted', 'last_voted', 'lastvoteddate', 'last_vote_date'],
        'status_code': ['statuscode', 'status', 'voter_status', 'registration_status']
    }
    
    # Address field patterns
    address_patterns = {
        'address_street_number': ['stnum', 'street_number', 'housenumber', 'regstnum', 'stnumber', 'address_number', 'streetno', 'house_number'],
        'address_street_fraction': ['stfrac', 'fraction', 'regstfrac', 'address_frac', 'street_fraction'],
        'address_street_pre_direction': ['stpredir', 'predirection', 'regstpredirection', 'address_dir_pre', 'streetdir', 'street_direction'],
        'address_street_name': ['stname', 'street_name', 'regstname', 'address_street', 'streetname', 'street'],
        'address_street_type': ['sttype', 'street_type', 'regsttype', 'address_suffix', 'streettype', 'street_suffix'],
        'address_unit_type': ['unittype', 'regunittype', 'address_unit_type', 'apartment_type'],
        'address_street_post_direction': ['stpostdir', 'postdirection', 'regstpostdirection', 'address_dir_post', 'street_post_dir'],
        'address_unit_number': ['unitnum', 'regstunitnum', 'address_unit', 'unitno', 'apartment_number']
    }
    
    # Mailing address patterns
    mailing_patterns = {
        'mailing_address': ['mail1', 'mailingaddress', 'mail_address', 'mail_addr'],
        'mailing_address2': ['mail2', 'mailingaddress2', 'mail_address2', 'mail_addr2'],
        'mailing_address3': ['mail3', 'mailingaddress3', 'mail_address3', 'mail_addr3'],
        'mailing_city': ['mailcity', 'mail_city'],
        'mailing_state': ['mailstate', 'mail_state'],
        'mailing_zip': ['mailzip', 'mail_zip', 'mailing_postal_code'],
        'mailing_country': ['mailcountry', 'mail_country']
    }
    
    mappings = {}
    
    # Create case-insensitive lookup for DataFrame columns
    df_col_lookup = {col.lower(): col for col in df.columns}
    
    # First map non-address fields with higher priority
    for schema_field, patterns in name_patterns.items():
        for col_lower, actual_col in df_col_lookup.items():
            # Check for exact matches first
            if any(pattern == col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
            # Then check for partial matches
            elif any(pattern in col_lower for pattern in patterns):
                # Skip if this column is already mapped
                if actual_col not in mappings:
                    mappings[actual_col] = schema_field
                    break
    
    # Then handle address fields
    for schema_field, patterns in address_patterns.items():
        for col_lower, actual_col in df_col_lookup.items():
            # Skip if this column is already mapped
            if actual_col in mappings:
                continue
            # Check for exact matches first
            if any(pattern == col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
            # Then check for partial matches
            elif any(pattern in col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
    
    # Finally handle mailing address fields
    for schema_field, patterns in mailing_patterns.items():
        for col_lower, actual_col in df_col_lookup.items():
            # Skip if this column is already mapped
            if actual_col in mappings:
                continue
            # Check for exact matches first
            if any(pattern == col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
            # Then check for partial matches
            elif any(pattern in col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
    
    return mappings

def analyze_address_fields(df: pd.DataFrame, column_names: List[str] = None) -> Dict[str, Any]:
    """
    Analyze data to determine order of address fields.
    
    Args:
        df: DataFrame containing voter data
        column_names: Optional list of original column names
        
    Returns:
        Dictionary containing address field configuration
    """
    # Common patterns for address components
    address_patterns = {
        'street_number': ['stnum', 'street_number', 'housenumber', 'regstnum', 'stnumber', 'address_number', 'streetno', 'res_street_number'],
        'street_fraction': ['stfrac', 'fraction', 'regstfrac', 'address_frac', 'res_street_fraction'],
        'street_pre_direction': ['stpredir', 'predirection', 'regstpredirection', 'address_dir_pre', 'streetdir', 'res_street_pre_direction'],
        'street_name': ['stname', 'street_name', 'regstname', 'address_street', 'streetname', 'res_street_name'],
        'street_type': ['sttype', 'street_type', 'regsttype', 'address_suffix', 'streettype', 'res_street_type'],
        'unit_type': ['unittype', 'regunittype', 'address_unit_type', 'res_unit_type'],
        'street_post_direction': ['stpostdir', 'postdirection', 'regstpostdirection', 'address_dir_post', 'res_street_post_direction'],
        'unit_number': ['unitnum', 'regstunitnum', 'address_unit', 'unitno', 'res_unit_number'],
        'city': ['city', 'regcity', 'res_city'],
        'state': ['state', 'regstate', 'res_state'],
        'zip': ['zip', 'zipcode', 'regzipcode', 'res_zip']
    }
    
    # Patterns for mailing address fields - we specifically want to exclude these
    mailing_patterns = [
        'mail', 'mailing', 'mailcity', 'mailstate', 'mailzip', 'mailcountry'
    ]
    
    # Voter ID patterns - we want to exclude these too
    voter_id_patterns = [
        'voterid', 'voter_id', 'statevoterid', 'voter', 'votid', 'id'
    ]
    
    # Use provided column names if available, otherwise get from DataFrame
    if column_names is None:
        column_names = df.columns.tolist()
    
    # First check for a single combined address field
    for col in column_names:
        if any(pattern in col.lower() for pattern in ['address_full', 'full_address', 'complete_address']):
            return {'address': {'fields': [col], 'separator': ' '}}
            
    # Define the correct order for address components
    address_component_order = [
        'street_number',
        'street_fraction',
        'street_pre_direction',
        'street_name',
        'street_type',
        'unit_type',
        'street_post_direction',
        'unit_number',
        'city',
        'state',
        'zip'
    ]
    
    # Find columns that match each address component in the correct order
    address_fields = []
    column_component_map = {}
    
    # First map columns to components
    for col in column_names:
        col_lower = col.lower()
        
        # Skip mailing address fields
        if any(pattern in col_lower for pattern in mailing_patterns):
            continue
            
        # Skip voter ID fields
        if any(pattern in col_lower for pattern in voter_id_patterns):
            continue
            
        # Map the column to an address component
        for component, patterns in address_patterns.items():
            if any(pattern in col_lower for pattern in patterns):
                column_component_map[col] = component
                break
    
    # Then add columns in the correct component order
    for component in address_component_order:
        for col, comp in column_component_map.items():
            if comp == component:
                address_fields.append(col)
                
    return {'address': {'fields': address_fields, 'separator': ' '}}

def create_state_config(state: str, file_path: str, mappings: Dict[str, str], df: pd.DataFrame, column_names: List[str], config_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a state configuration dictionary and save it to a file.
    
    Args:
        state: Two-letter state code
        file_path: Path to sample voter data file
        mappings: Dictionary mapping state columns to common schema fields
        df: Sample DataFrame
        column_names: List of column names from the file
        config_dir: Directory to save config file (optional)
    
    Returns:
        Dict containing the state configuration
    """
    # Detect file format and delimiter
    file_format, delimiter, _ = detect_file_format(file_path)
    
    # Create config dictionary
    config = {
        'state_code': state.upper(),
        'file_format': file_format,
        'delimiter': delimiter,
        'column_mappings': mappings,
        'address_fields': analyze_address_fields(df, column_names),
        'created_at': datetime.now().isoformat(),
        'last_updated': datetime.now().isoformat(),
        'column_names': column_names
    }
    
    # Save config to file
    if config_dir is None:
        config_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'configs')
    
    os.makedirs(config_dir, exist_ok=True)
    config_file = os.path.join(config_dir, f'{state.lower()}_config.json')
    
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    return config

def save_config(config: Dict, state_code: str):
    """
    Save state configuration to YAML file.
    
    Args:
        config: Configuration dictionary
        state_code: Two-letter state code
    """
    config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
    os.makedirs(config_dir, exist_ok=True)
    
    config_path = os.path.join(config_dir, f'{state_code.lower()}_config.yaml')
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

def onboard_state(args: argparse.Namespace) -> None:
    """
    Onboard a new state by analyzing data file and generating config.
    
    Args:
        args: Command line arguments
    """
    # Detect the delimiter
    with open(args.file, 'r', encoding='windows-1252') as f:
        first_line = f.readline()
        if '|' in first_line:
            delimiter = '|'
        elif ',' in first_line:
            delimiter = ','
        else:
            delimiter = ','  # Default to comma
    
    # Read sample of data file to analyze
    df = pd.read_csv(args.file, nrows=1000, delimiter=delimiter, encoding='windows-1252')
    
    # Clean up column names by removing trailing whitespace
    df.columns = [col.strip() for col in df.columns]
    
    # Get column names from the file
    column_names = df.columns.tolist()
    
    # Analyze columns and suggest mappings
    mappings = analyze_columns(df)
    
    # Analyze address fields
    address_fields = analyze_address_fields(df, column_names)
    
    # Create config
    config = {
        'state_code': args.state,
        'file_format': 'csv',
        'delimiter': delimiter,
        'column_mappings': mappings,
        'address_fields': address_fields,
        'column_names': column_names  # Add column names to config
    }
    
    # Create config directory if it doesn't exist
    config_dir = args.config_dir or os.path.join(os.path.dirname(__file__), '..', '..', '..', 'configs')
    os.makedirs(config_dir, exist_ok=True)
    
    # Write config file
    config_file = os.path.join(config_dir, f'{args.state.lower()}_config.json')
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=4)
    
    print(f"Created config file: {config_file}")

def main():
    """Main entry point for onboarding a state's voter data format."""
    parser = argparse.ArgumentParser(description='Onboard a new state voter data format')
    parser.add_argument('state', help='Two-letter state code')
    parser.add_argument('file', help='Path to sample voter data file')
    parser.add_argument('--force', action='store_true', help='Force overwrite existing config')
    parser.add_argument('--config_dir', help='Directory to save config file', default=None)
    
    args = parser.parse_args()
    onboard_state(args)
    return 0

if __name__ == '__main__':
    sys.exit(main()) 