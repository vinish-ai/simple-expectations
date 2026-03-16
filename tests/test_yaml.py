from simple_expectations.core.suite import ExpectationSuite
import tempfile
import yaml
import os

def test_yaml_roundtrip():
    yaml_content = """
name: test_suite
data_sources:
  - name: my_duckdb
    backend: duckdb
    kwargs:
      database: my_data.db
expectations:
  - type: expect_column_to_exist
    kwargs:
      column: id
  - type: expect_column_values_to_be_between
    kwargs:
      column: age
      min_value: 0
      max_value: 100
"""
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml") as f:
        f.write(yaml_content)
        temp_file = f.name
        
    try:
        suite = ExpectationSuite.from_yaml(temp_file)
        assert suite.name == "test_suite"
        assert len(suite.data_sources) == 1
        assert suite.data_sources[0].name == "my_duckdb"
        assert suite.data_sources[0].backend == "duckdb"
        assert suite.data_sources[0].kwargs["database"] == "my_data.db"
        assert len(suite.expectations) == 2
        assert suite.expectations[0].type == "expect_column_to_exist"
        assert suite.expectations[1].kwargs["max_value"] == 100
    finally:
        os.remove(temp_file)
