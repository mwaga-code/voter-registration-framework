#!/usr/bin/env python3
"""
Test runner for voter registration framework tests.
Discovers and runs all tests in the tests directory.
"""

import os
import sys
import unittest
import datetime

def run_tests():
    """
    Run all tests for the voter registration framework.
    """
    start_time = datetime.datetime.now()
    
    # Add the parent directory to the path so imports work correctly
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(base_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    # Run all tests in the unit and integration directories
    unit_dir = os.path.join(base_dir, 'unit')
    integration_dir = os.path.join(base_dir, 'integration')
    
    test_suite = unittest.TestSuite()
    
    # Manually load the tests to avoid unittest discovery path issues
    if os.path.exists(unit_dir):
        print(f"Loading tests from {unit_dir}")
        for file in os.listdir(unit_dir):
            if file.startswith('test_') and file.endswith('.py'):
                module_name = f"tests.unit.{file[:-3]}"
                try:
                    module = __import__(module_name, fromlist=['*'])
                    for item in dir(module):
                        if item.startswith('Test'):
                            test_case = getattr(module, item)
                            if isinstance(test_case, type) and issubclass(test_case, unittest.TestCase):
                                test_suite.addTest(unittest.makeSuite(test_case))
                except ImportError as e:
                    print(f"Error importing {module_name}: {e}")
    
    if os.path.exists(integration_dir):
        print(f"Loading tests from {integration_dir}")
        for file in os.listdir(integration_dir):
            if file.startswith('test_') and file.endswith('.py'):
                module_name = f"tests.integration.{file[:-3]}"
                try:
                    module = __import__(module_name, fromlist=['*'])
                    for item in dir(module):
                        if item.startswith('Test'):
                            test_case = getattr(module, item)
                            if isinstance(test_case, type) and issubclass(test_case, unittest.TestCase):
                                test_suite.addTest(unittest.makeSuite(test_case))
                except ImportError as e:
                    print(f"Error importing {module_name}: {e}")
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Calculate elapsed time
    end_time = datetime.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    
    # Print summary
    print("\n=== TEST RESULTS ===")
    print(f"Ran {result.testsRun} tests in {elapsed_time:.3f}s")
    print(f"Errors: {len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Skipped: {len(result.skipped)}")
    
    # Print errors
    if result.errors:
        print("\n=== ERRORS ===")
        for test, error in result.errors:
            print(f"\n{test}")
            print(f"{error}")
    
    # Print failures
    if result.failures:
        print("\n=== FAILURES ===")
        for test, failure in result.failures:
            print(f"\n{test}")
            print(f"{failure}")
    
    return result

if __name__ == "__main__":
    result = run_tests()
    sys.exit(not result.wasSuccessful()) 