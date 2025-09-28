#!/usr/bin/env python3
"""
Simple connection tester for Informatica to Databricks migration testing.

This script demonstrates basic configuration validation without external dependencies.
"""

import json
import sys
import logging
import os
from typing import Dict, Any

def setup_logging():
    """Configure logging for test execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def validate_json_config(config_file: str) -> bool:
    """Validate that a JSON configuration file is valid."""
    try:
        if not os.path.exists(config_file):
            logging.error(f"Configuration file not found: {config_file}")
            return False
            
        with open(config_file, 'r') as f:
            config = json.load(f)
            
        logging.info(f"Successfully loaded configuration from {config_file}")
        logging.info(f"Configuration keys: {list(config.keys())}")
        return True
        
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {config_file}: {e}")
        return False
    except Exception as e:
        logging.error(f"Error reading {config_file}: {e}")
        return False

def test_sample_data_structure() -> bool:
    """Test that sample data files exist and are readable."""
    sample_data_dir = "sample-data"
    
    if not os.path.exists(sample_data_dir):
        logging.error(f"Sample data directory not found: {sample_data_dir}")
        return False
    
    sample_files = [f for f in os.listdir(sample_data_dir) if f.endswith('.csv')]
    
    if not sample_files:
        logging.warning("No CSV sample files found")
        return True
    
    for file in sample_files:
        file_path = os.path.join(sample_data_dir, file)
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
                logging.info(f"Sample file {file}: {len(lines)} lines")
        except Exception as e:
            logging.error(f"Error reading sample file {file}: {e}")
            return False
    
    return True

def run_basic_tests() -> bool:
    """Run basic repository structure tests."""
    logging.info("Starting basic repository tests...")
    
    results = []
    
    # Test configuration files
    config_files = [
        "configs/informatica-connection.json",
        "configs/databricks-connection.json"
    ]
    
    for config_file in config_files:
        results.append(validate_json_config(config_file))
    
    # Test sample data structure
    results.append(test_sample_data_structure())
    
    return all(results)

if __name__ == "__main__":
    setup_logging()
    
    logging.info("Starting repository validation tests...")
    
    success = run_basic_tests()
    
    if success:
        logging.info("All basic tests passed! Repository structure is valid.")
        sys.exit(0)
    else:
        logging.error("Some tests failed!")
        sys.exit(1)