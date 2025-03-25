# Voter Registration Data Framework

A Python framework for processing and normalizing voter registration data from different states.

## Features

- State-specific data adapters for different voter registration formats
- Data normalization to a common format
- Data quality analysis and reporting
- Schema validation and mapping tools
- Support for multiple data sources (CSV, Excel, APIs)
- Dynamic column mapping and address field detection
- Consistent address formatting
- Duplicate voter ID detection
- Single SQLite database for multi-state data storage

## Installation

```bash
pip install -r requirements.txt
```

## Project Structure

```
voter-registration-framework/
├── src/
│   └── voter_framework/
│       ├── core/           # Core framework components
│       ├── adapters/       # State-specific data adapters
│       ├── normalizers/    # Data normalization tools
│       ├── cli/            # Command-line interface tools
│       └── utils/          # Utility functions
├── tests/
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── fixtures/          # Test data fixtures
├── docs/                  # Documentation
├── configs/              # State-specific configuration files
└── data/                 # Data directory for SQLite database and source files
```

## Usage

### Onboarding New State Data

The onboarding process analyzes a state's voter data file and creates a configuration file for importing:

```bash
python -m voter_framework.cli.onboard_state WA data/WA/voter_data.txt
```

This creates a configuration file in the `configs` directory (e.g., `configs/wa_config.json`).

### Importing Data to SQLite

Import voter data into the SQLite database:

```bash
# Basic import
python -m voter_framework.cli.import_to_sqlite WA data/WA/voter_data.txt

# Import with verbose output
python -m voter_framework.cli.import_to_sqlite WA data/WA/voter_data.txt --verbose

# Import with custom configuration directory
python -m voter_framework.cli.import_to_sqlite WA data/WA/voter_data.txt --config-dir /path/to/configs

# Import with custom database path
python -m voter_framework.cli.import_to_sqlite WA data/WA/voter_data.txt --db /path/to/voters.db
```

The data is imported into a single SQLite database (`data/voters.db`) with state-specific tables. For example:
- Washington data goes into `voters_wa_*` tables
- Oregon data goes into `voters_or_*` tables
- California data goes into `voters_ca_*` tables

### Data Quality Features

- Automatic detection and prevention of duplicate voter IDs
- Case-insensitive column mapping
- Consistent address formatting
- Data validation during import
- Detailed error reporting for data issues

### Querying Voter Data

Example SQLite queries:

```sql
-- Get all fields for a specific voter
SELECT * FROM voters_wa_20250303_VRDB_Extract_20250325 
WHERE voter_id = '14204440';

-- Count voters by city
SELECT city, COUNT(*) as count 
FROM voters_wa_20250303_VRDB_Extract_20250325 
GROUP BY city 
ORDER BY count DESC;
```

## Running Tests

The framework includes comprehensive unit and integration tests:

```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/unit/test_address_detection.py

# Run with verbose output
python -m pytest -v

# Run specific test
python -m pytest tests/test_import_duplicates.py::test_duplicate_voter_ids
```

Test data fixtures for Washington, Oregon, and California formats are included in the `tests/fixtures` directory.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
6. Add unit and integration tests for new functionality

## License

MIT License - see LICENSE file for details 