import argparse
import sys
from pathlib import Path

from simple_expectations.core.context import Context
from simple_expectations.core.suite import ExpectationSuite
from simple_expectations.core.reporter import TextReporter

def main():
    parser = argparse.ArgumentParser(description="Simple Expectations CLI Runner")
    parser.add_argument("command", choices=["validate"], help="Command to run")
    parser.add_argument("suite", help="Path to the ExpectationSuite YAML file")
    
    args = parser.parse_args()
    
    if args.command == "validate":
        filepath = Path(args.suite)
        if not filepath.exists():
            print(f"Error: Suite file '{filepath}' not found.")
            sys.exit(1)
            
        suite = ExpectationSuite.from_yaml(filepath)
        if not suite.data_sources:
            print("Error: The YAML suite must define at least one `data_source` directly within it to use the CLI runner.")
            sys.exit(1)
            
        # By default, validating the first table bound in the YAML
        primary_source = suite.data_sources[0]
        if not primary_source.table_name:
            print(f"Error: Data source '{primary_source.name}' must define a `table_name` hint in the YAML to use the CLI.")
            sys.exit(1)
            
        context = Context()
        context.add_data_source_from_suite(suite)
        
        table = context.get_table(primary_source.name, primary_source.table_name)
        
        print(f"Evaluating {len(suite.expectations)} expectations...")
        results = context.validate(table, suite)
        
        reporter = TextReporter(results)
        reporter.print_report()
        
        if results.success:
            sys.exit(0)
        else:
            sys.exit(1)

if __name__ == "__main__":
    main()
