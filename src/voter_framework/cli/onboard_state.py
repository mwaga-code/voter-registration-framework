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
        'first_name': ['first', 'given', 'fname', 'firstname', 'name_first'],
        'last_name': ['last', 'surname', 'lname', 'lastname', 'name_last'],
        'middle_name': ['middle', 'mname', 'middlename', 'name_middle'],
        'birth_year': ['birth', 'dob', 'birthdate', 'date_of_birth', 'birthyear'],
        'birthday': ['birthday', 'day_of_birth'],
        'registration_date': ['registrationdate', 'regdate', 'registration_date'],
        'city': ['city', 'town', 'regcity'],
        'state': ['state', 'regstate'],
        'zip_code': ['zip', 'postal', 'zipcode', 'regzipcode'],
        'gender': ['sex', 'gender'],
        'party': ['party', 'political', 'affiliation'],
        'precinct': ['precinct', 'district', 'precinctcode', 'precinct_id'],
        'county': ['county', 'parish', 'countycode', 'county_name'],
        'voter_id': ['voterid', 'voter_id', 'statevoterid', 'voter', 'votid', 'id'],
        'legislative_district': ['legislativedistrict', 'legdistrict'],
        'congressional_district': ['congressionaldistrict', 'congdistrict'],
        'last_voted_date': ['lastvoted', 'last_voted', 'lastvoteddate'],
        'status_code': ['statuscode', 'status', 'voter_status']
    }
    
    # Address field patterns
    address_patterns = {
        'address_street_number': ['stnum', 'street_number', 'housenumber', 'regstnum', 'stnumber', 'address_number', 'streetno'],
        'address_street_fraction': ['stfrac', 'fraction', 'regstfrac', 'address_frac'],
        'address_street_pre_direction': ['stpredir', 'predirection', 'regstpredirection', 'address_dir_pre', 'streetdir'],
        'address_street_name': ['stname', 'street_name', 'regstname', 'address_street', 'streetname'],
        'address_street_type': ['sttype', 'street_type', 'regsttype', 'address_suffix', 'streettype'],
        'address_unit_type': ['unittype', 'regunittype', 'address_unit_type'],
        'address_street_post_direction': ['stpostdir', 'postdirection', 'regstpostdirection', 'address_dir_post'],
        'address_unit_number': ['unitnum', 'regstunitnum', 'address_unit', 'unitno']
    }
    
    # Mailing address patterns
    mailing_patterns = {
        'mailing_address': ['mail1', 'mailingaddress'],
        'mailing_address2': ['mail2', 'mailingaddress2'],
        'mailing_address3': ['mail3', 'mailingaddress3'],
        'mailing_city': ['mailcity'],
        'mailing_state': ['mailstate'],
        'mailing_zip': ['mailzip'],
        'mailing_country': ['mailcountry']
    }
    
    # State-specific mappings
    state_mappings = {
        'WA': {
            'FName': 'first_name',
            'LName': 'last_name',
            'MName': 'middle_name',
            'StateVoterID': 'voter_id',
            'Birthyear': 'birth_year',
            'Gender': 'gender',
            'PrecinctCode': 'precinct',
            'CountyCode': 'county',
            'LegislativeDistrict': 'legislative_district',
            'CongressionalDistrict': 'congressional_district',
            'LastVoted': 'last_voted_date',
            'StatusCode': 'status_code',
            'RegStNum': 'address_street_number',
            'RegStFrac': 'address_street_fraction',
            'RegStPreDirection': 'address_street_pre_direction',
            'RegStName': 'address_street_name',
            'RegStType': 'address_street_type',
            'RegUnitType': 'address_unit_type',
            'RegStPostDirection': 'address_street_post_direction',
            'RegStUnitNum': 'address_unit_number',
            'Mail1': 'mailing_address',
            'Mail2': 'mailing_address2',
            'Mail3': 'mailing_address3',
            'MailCity': 'mailing_city',
            'MailState': 'mailing_state',
            'MailZip': 'mailing_zip',
            'MailCountry': 'mailing_country',
            'Registrationdate': 'registration_date',
            'RegCity': 'city',
            'RegState': 'state',
            'RegZipCode': 'zip_code'
        },
        'OR': {
            'VoterId': 'voter_id',
            'FirstName': 'first_name',
            'LastName': 'last_name',
            'MiddleName': 'middle_name',
            'BirthYear': 'birth_year',
            'RegDate': 'registration_date',
            'City': 'city',
            'State': 'state',
            'Zip': 'zip_code',
            'Sex': 'gender',
            'Precinct': 'precinct',
            'County': 'county',
            'LegDistrict': 'legislative_district',
            'CongDistrict': 'congressional_district',
            'LastVotedDate': 'last_voted_date',
            'Status': 'status_code',
            'StreetNo': 'address_street_number',
            'StreetDir': 'address_street_pre_direction',
            'StreetName': 'address_street_name',
            'StreetType': 'address_street_type',
            'UnitType': 'address_unit_type',
            'UnitNo': 'address_unit_number',
            'MailAddress': 'mailing_address',
            'MailCity': 'mailing_city',
            'MailState': 'mailing_state',
            'MailZip': 'mailing_zip'
        },
        'CA': {
            'ID': 'voter_id',
            'NAME_FIRST': 'first_name',
            'NAME_LAST': 'last_name',
            'NAME_MIDDLE': 'middle_name',
            'NAME_SUFFIX': 'suffix',
            'BIRTH_YEAR': 'birth_year',
            'GENDER': 'gender',
            'ADDRESS_FULL': 'address',
            'CITY': 'city',
            'STATE': 'state',
            'ZIP': 'zip_code',
            'COUNTY_NAME': 'county',
            'PRECINCT_ID': 'precinct',
            'ASSEMBLY_DISTRICT': 'legislative_district',
            'CONGRESS_DISTRICT': 'congressional_district',
            'LAST_VOTED_DATE': 'last_voted_date',
            'VOTER_STATUS': 'status_code',
            'MAILING_ADDRESS': 'mailing_address',
            'MAILING_CITY': 'mailing_city',
            'MAILING_STATE': 'mailing_state',
            'MAILING_ZIP': 'mailing_zip',
            'REGISTRATION_DATE': 'registration_date'
        }
    }
    
    mappings = {}
    
    # First try state-specific mappings
    for state, state_map in state_mappings.items():
        # Check if this is the right state format by looking for key columns
        key_columns = {
            'WA': ['FName', 'LName', 'StateVoterID'],
            'OR': ['FirstName', 'LastName', 'VoterId'],
            'CA': ['NAME_FIRST', 'NAME_LAST', 'ID']
        }
        # Create case-insensitive lookup for DataFrame columns
        df_col_lookup = {col.lower(): col for col in df.columns}
        # Check if all key columns exist (case-insensitive)
        if all(any(key.lower() == col_lower for col_lower in df_col_lookup) for key in key_columns[state]):
            # This is the right state format, use its mappings
            # Create case-sensitive mappings using the actual column names from the DataFrame
            case_sensitive_mappings = {}
            for orig_col, schema_field in state_map.items():
                # Find the actual column name in the DataFrame that matches case-insensitively
                actual_col = next((df_col_lookup[col_lower] for col_lower in df_col_lookup 
                                 if col_lower == orig_col.lower()), None)
                if actual_col:
                    case_sensitive_mappings[actual_col] = schema_field
            mappings.update(case_sensitive_mappings)
            return mappings
    
    # If no state-specific format matched, use pattern matching
    # Create case-insensitive lookup for DataFrame columns
    df_col_lookup = {col.lower(): col for col in df.columns}
    
    # First map non-address fields
    for schema_field, patterns in name_patterns.items():
        for col_lower, actual_col in df_col_lookup.items():
            if any(pattern in col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
    
    # Then handle address fields
    for schema_field, patterns in address_patterns.items():
        for col_lower, actual_col in df_col_lookup.items():
            if any(pattern in col_lower for pattern in patterns):
                mappings[actual_col] = schema_field
                break
    
    # Finally handle mailing address fields
    for schema_field, patterns in mailing_patterns.items():
        for col_lower, actual_col in df_col_lookup.items():
            if any(pattern in col_lower for pattern in patterns):
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