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
├── docs/                   # Documentation
└── examples/              # Example usage
```

## Usage

### Onboarding New State Data

```bash
python -m src.voter_framework.cli.onboard_state WA data/WA/voter_data.txt
```

### Importing Data to SQLite

```bash
python -m src.voter_framework.cli.import_to_sqlite WA data/WA/voter_data.txt
```

### Processing Voter Data

```python
from src.voter_framework.core import VoterDataProcessor
from src.voter_framework.adapters import WAStateAdapter, ORStateAdapter

# Initialize adapters
wa_adapter = WAStateAdapter()
or_adapter = ORStateAdapter()

# Process data
processor = VoterDataProcessor()
wa_data = processor.process_state_data(wa_adapter)
or_data = processor.process_state_data(or_adapter)
```

## Running Tests

The framework includes comprehensive unit and integration tests to ensure reliability:

```bash
# Run all tests
./tests/run_tests.py

# Run specific test file
python -m unittest tests/unit/test_address_detection.py

# Run specific test class
python -m unittest tests.unit.test_address_detection.TestAddressDetection

# Run specific test method
python -m unittest tests.unit.test_address_detection.TestAddressDetection.test_wa_address_field_detection
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