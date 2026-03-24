import argparse
import sys
import json
from pathlib import Path

from dqe.core.context import Context
from dqe.core.suite import ExpectationSuite
from dqe.core.reporter import TextReporter
from dqe.core.profiler import Profiler

def main():
    parser = argparse.ArgumentParser(description="Data Quality Engine CLI")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to run")
    
    # init
    parser_init = subparsers.add_parser("init", help="Initialize a generic boilerplate configuration")
    
    # validate
    parser_val = subparsers.add_parser("validate", help="Validate tables using an ExpectationSuite YAML file")
    parser_val.add_argument("suite", help="Path to the ExpectationSuite YAML file")
    
    # profile
    parser_prof = subparsers.add_parser("profile", help="Profile a dataset and automatically generate a suite")
    parser_prof.add_argument("--backend", required=True, help="Backend type to use (e.g. duckdb, pandas)")
    parser_prof.add_argument("--table", required=True, help="Name of the table to read and profile")
    parser_prof.add_argument("--kwargs", default="{}", help="JSON string representing kwargs connection details")
    parser_prof.add_argument("--out", help="Optional output YAML file path for the suite")
    
    args = parser.parse_args()
    
    if args.command == "init":
        suite_path = Path("my_validations.yaml")
        if suite_path.exists():
            print(f"Error: {suite_path} already exists.")
            sys.exit(1)
            
        yaml_content = """name: "my_suite"\n\ndata_sources:\n  - name: "my_duckdb"\n    backend: "duckdb"\n    kwargs:\n      database: "my_data.db"\n    table_name: "my_table"\n\nexpectations:\n  - type: "expect_column_to_exist"\n    kwargs:\n      column: "id"\n"""
        suite_path.write_text(yaml_content)
        
        script_path = Path("run_validations.py")
        if not script_path.exists():
            py_content = """import dqe\n\ncontext = dqe.Context()\nsuite = dqe.ExpectationSuite.from_yaml("my_validations.yaml")\ncontext.add_data_source_from_suite(suite)\n\n# table = context.get_table("my_duckdb", "my_table")\n# results = context.validate(table, suite)\n# print(f"Success: {results.success}")\n"""
            script_path.write_text(py_content)
            
        print("Initialized DQE project.")
        print(f"Created boilerplate in {suite_path} and {script_path}")
        sys.exit(0)
        
    elif args.command == "validate":
        filepath = Path(args.suite)
        if not filepath.exists():
            print(f"Error: Suite file '{filepath}' not found.")
            sys.exit(1)
            
        suite = ExpectationSuite.from_yaml(filepath)
        if not suite.data_sources:
            print("Error: The YAML suite must define at least one `data_source` directly within it to use the CLI.")
            sys.exit(1)
            
        primary_source = suite.data_sources[0]
        if not primary_source.table_name:
            print(f"Error: Data source '{primary_source.name}' must define a `table_name` in the YAML to use the CLI.")
            sys.exit(1)
            
        context = Context()
        context.add_data_source_from_suite(suite)
        table = context.get_table(primary_source.name, primary_source.table_name)
        
        print(f"Evaluating {len(suite.expectations)} expectations...")
        results = context.validate(table, suite)
        
        reporter = TextReporter(results)
        reporter.print_report()
        
        sys.exit(0 if results.success else 1)
        
    elif args.command == "profile":
        try:
            conn_kwargs = json.loads(args.kwargs)
        except json.JSONDecodeError as e:
            print(f"Error parsing kwargs JSON: {e}")
            sys.exit(1)
            
        print(f"Connecting to '{args.backend}' and fetching '{args.table}'...")
        context = Context()
        context.add_data_source("profile_source", backend=args.backend, **conn_kwargs)
        
        try:
            table = context.get_table("profile_source", args.table)
        except Exception as e:
            print(f"Error fetching table '{args.table}': {e}")
            sys.exit(1)
            
        print("Profiling table metrics and computing baseline expectations...")
        suite = Profiler.profile_table(table, suite_name=f"{args.table}_profile")
        
        yaml_out = suite.to_yaml()
        if args.out:
            Path(args.out).write_text(yaml_out)
            print(f"Successfully generated suite YAML to: {args.out}")
        else:
            print("\n" + yaml_out)
        
        sys.exit(0)

if __name__ == "__main__":
    main()
