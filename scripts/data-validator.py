#!/usr/bin/env python3
"""
Sample data validation script for Informatica to Databricks migration testing.

This script demonstrates basic data validation patterns for testing
migration accuracy and data integrity.
"""

import pandas as pd
import sys
import logging
from typing import Dict, Any

def setup_logging():
    """Configure logging for test execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def validate_row_count(source_df: pd.DataFrame, target_df: pd.DataFrame) -> bool:
    """Validate that row counts match between source and target."""
    source_count = len(source_df)
    target_count = len(target_df)
    
    logging.info(f"Source row count: {source_count}")
    logging.info(f"Target row count: {target_count}")
    
    if source_count != target_count:
        logging.error(f"Row count mismatch: {source_count} vs {target_count}")
        return False
    
    logging.info("Row count validation passed")
    return True

def validate_schema(source_df: pd.DataFrame, target_df: pd.DataFrame) -> bool:
    """Validate that schemas match between source and target."""
    source_columns = set(source_df.columns)
    target_columns = set(target_df.columns)
    
    if source_columns != target_columns:
        missing_in_target = source_columns - target_columns
        extra_in_target = target_columns - source_columns
        
        if missing_in_target:
            logging.error(f"Columns missing in target: {missing_in_target}")
        if extra_in_target:
            logging.warning(f"Extra columns in target: {extra_in_target}")
        
        return False
    
    logging.info("Schema validation passed")
    return True

def run_validation_tests(source_file: str, target_file: str) -> bool:
    """Run all validation tests."""
    try:
        # Load data
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)
        
        # Run validations
        results = []
        results.append(validate_row_count(source_df, target_df))
        results.append(validate_schema(source_df, target_df))
        
        # Return overall result
        return all(results)
        
    except Exception as e:
        logging.error(f"Validation failed with error: {e}")
        return False

if __name__ == "__main__":
    setup_logging()
    
    if len(sys.argv) != 3:
        print("Usage: python data-validator.py <source_file> <target_file>")
        sys.exit(1)
    
    source_file = sys.argv[1]
    target_file = sys.argv[2]
    
    logging.info(f"Starting validation: {source_file} -> {target_file}")
    
    success = run_validation_tests(source_file, target_file)
    
    if success:
        logging.info("All validation tests passed!")
        sys.exit(0)
    else:
        logging.error("Validation tests failed!")
        sys.exit(1)