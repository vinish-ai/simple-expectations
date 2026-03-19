import argparse
import sys
from pathlib import Path

from simple_expectations.core.context import Context
from simple_expectations.core.suite import ExpectationSuite
from simple_expectations.core.reporter import TextReporter

def main():
    parser = argparse.ArgumentParser(description="Simple Expectations CLI Runner")
    parser.add_argument("command", choices=["validate", "init"], help="Command to run")
    parser.add_argument("suite", nargs="?", help="Path to the ExpectationSuite YAML file (required for validate)")
    
    args = parser.parse_args()
    
    if args.command == "init":
        suite_path = Path("my_validations.yaml")
        if suite_path.exists():
            print(f"Error: {suite_path} already exists.")
            sys.exit(1)
            
        yaml_content = """name: "my_suite"\n\ndata_sources:\n  - name: "my_duckdb"\n    backend: "duckdb"\n    kwargs:\n      database: "my_data.db"\n\nexpectations:\n  - type: "expect_column_to_exist"\n    kwargs:\n      column: "id"\n"""
        suite_path.write_text(yaml_content)
        
        script_path = Path("run_validations.py")
        if not script_path.exists():
            py_content = """import simple_expectations as se\n\ncontext = se.Context()\nsuite = se.ExpectationSuite.from_yaml("my_validations.yaml")\ncontext.add_data_source_from_suite(suite)\n\n# Assuming you have a table 'my_table' in your DB:\n# table = context.get_table("my_duckdb", "my_table")\n# results = context.validate(table, suite)\n# print(f"Success: {results.success}")\n"""
            script_path.write_text(py_content)
            
        print("Initialized Simple Expectations project.")
        print(f"Created boilerplate in {suite_path} and {script_path}")
        sys.exit(0)
        
    if args.command == "validate":
        if not args.suite:
            parser.error("The 'validate' command requires a suite file argument.")
            
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
