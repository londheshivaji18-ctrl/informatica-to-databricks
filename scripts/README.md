# Utility and Validation Scripts

This directory contains utility scripts for Informatica to Databricks migration testing.

## Script Categories

### Data Generation Scripts
- `generate-test-data.py` - Generate synthetic test datasets
- `create-sample-schemas.py` - Create sample database schemas
- `data-profiler.py` - Profile existing data sources

### Validation Scripts
- `data-validator.py` - Validate data after migration
- `schema-comparator.py` - Compare source vs target schemas
- `performance-analyzer.py` - Analyze migration performance

### Utility Scripts
- `connection-tester.py` - Test database connections
- `config-validator.py` - Validate configuration files
- `cleanup-resources.py` - Clean up test resources

## Usage

Each script includes:
- Command-line interface
- Configuration file support
- Detailed logging
- Error handling and recovery

Run `python <script-name> --help` for specific usage instructions.