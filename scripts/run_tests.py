#!/usr/bin/env python
"""Run all tests and generate coverage report."""

import sys
import subprocess
import os
from pathlib import Path

# Add project to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_tests():
    """Run all tests with coverage."""
    print("=" * 80)
    print("Running Dealdriver HubSpot Tests")
    print("=" * 80)
    
    # Test categories
    test_suites = [
        ("Unit Tests", "tests/unit"),
        ("Integration Tests", "tests/integration"),
        ("End-to-End Tests", "tests/e2e")
    ]
    
    all_passed = True
    
    for suite_name, test_dir in test_suites:
        print(f"\n{'=' * 40}")
        print(f"Running {suite_name}")
        print(f"{'=' * 40}")
        
        cmd = [
            "python", "-m", "pytest",
            test_dir,
            "-v",
            "--tb=short",
            "--cov=src",
            "--cov-append",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov"
        ]
        
        result = subprocess.run(cmd, cwd=project_root)
        
        if result.returncode != 0:
            all_passed = False
            print(f"\n❌ {suite_name} FAILED")
        else:
            print(f"\n✅ {suite_name} PASSED")
    
    # Generate final coverage report
    print("\n" + "=" * 80)
    print("Coverage Summary")
    print("=" * 80)
    
    subprocess.run([
        "python", "-m", "coverage", "report",
        "--show-missing"
    ], cwd=project_root)
    
    print("\n" + "=" * 80)
    if all_passed:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed. Please check the output above.")
    print("=" * 80)
    
    return 0 if all_passed else 1


def run_specific_test(test_file):
    """Run a specific test file."""
    cmd = [
        "python", "-m", "pytest",
        test_file,
        "-v",
        "--tb=short"
    ]
    
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test file
        exit_code = run_specific_test(sys.argv[1])
    else:
        # Run all tests
        exit_code = run_tests()
    
    sys.exit(exit_code)