# Simple-Expectations

**Simple-Expectations** is a modern, lightweight data validation library inspired by Great Expectations. 
It provides a completely stateless, embeddable, and high-performance validation engine powered by [Ibis](https://ibis-project.org/).

Because all validation rules are compiled into deferred Ibis expressions, `simple-expectations` evaluates checks symmetrically across **20+ Execution Backends**, including:
- DuckDB
- Polars
- Pandas
- PySpark
- Snowflake
- BigQuery
- PostgreSQL

## Features
- **Zero Boilerplate**: No deeply nested uncommitted directories or complex CLI initializations required. It's just a Python library you import.
- **SQL Pushdown**: Complex validations execute directly where your data lives (in the database or dataframe engine). Maximize speed by evaluating everything in a single SQL pass!
- **Pure Pydantic & YAML**: Validation suites are built entirely with strict Pydantic models. We support seamless definition via YAML configuration files.

## Installation
```bash
pip install simple-expectations
```

*Note: Depending on your choice of execution environment, you'll need the corresponding client installed (e.g. `pip install duckdb pandas polars`).*

## Quick Start (Python API)
You can directly construct everything in pure Python objects. This makes embedding validations into DAG workflows (like Airflow or Dagster) effortless!

```python
import pandas as pd
import simple_expectations as se

# Create a Context and register your data sources
context = se.Context()

# Example: Feed it a pandas dataframe (automatically routed to rapid in-memory DuckDB!)
df = pd.DataFrame({"id": [1, 2, 3], "age": [25, 30, None]})
context.add_data_source("my_db", backend="pandas", dictionary={"users": df})

# Let's say we instead wanted to connect straight to a Postgres or Snowflake warehouse:
# context.add_data_source("my_db", backend="postgres", host=..., database=...)

# Read the table from the Context
table = context.get_table("my_db", "users")

# Build the Expectation Suite
suite = se.ExpectationSuite(
    name="users_suite",
    expectations=[
        se.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
        se.BaseExpectation(
            type="expect_column_values_to_be_between", 
            kwargs={"column": "age", "min_value": 0, "max_value": 100}
        )
    ]
)

# Validate!
results = context.validate(table, suite)
print(f"Validation Success: {results.success}")
```

## Quick Start (YAML Configuration)

The real power comes from abstracting rules (and backend connection definitions) out of your code entirely and into maintainable YAML files.

**my_validations.yaml**:
```yaml
name: "my_suite"

# Optional: You can declare the Data Source connection directly in the YAML
data_sources:
  - name: "primary_warehouse"
    backend: "duckdb"
    kwargs:
      database: "my_data.db"

# Define the rules
expectations:
  - type: "expect_column_to_exist"
    kwargs:
      column: "id"
  - type: "expect_column_values_to_not_be_null"
    kwargs:
      column: "id"
  - type: "expect_column_values_to_be_between"
    kwargs:
      column: "age"
      min_value: 0
      max_value: 100
```

**app.py**:
```python
import simple_expectations as se

context = se.Context()
suite = se.ExpectationSuite.from_yaml("my_validations.yaml")

# Automatically provision the duckdb "primary_warehouse" source defined in the YAML!
context.add_data_source_from_suite(suite)

# Read the table and evaluate
table = context.get_table("primary_warehouse", "users")
results = context.validate(table, suite)

print(results.model_dump_json(indent=2))
```

## CLI Interface
You can quickly generate boilerplate validation suites and run them directly from the command line:
```bash
# Generate a starter my_validations.yaml and run_validations.py script
simple-expectations init

# Validate an existing suite YAML
simple-expectations validate my_validations.yaml
```

## Available Expectations
- **Table Structure:**
  - `expect_table_row_count_to_be_between(min_value=None, max_value=None)`
  - `expect_table_columns_to_match_set(column_set, exact_match=True)`
  - `expect_table_columns_to_match_ordered_list(column_list)`
- **Column Structure:**
  - `expect_column_to_exist(column)`
  - `expect_column_values_to_be_unique(column)`
- **Column Map (Row-level):**
  - `expect_column_values_to_be_null(column, mostly=1.0)`
  - `expect_column_values_to_not_be_null(column, mostly=1.0)`
  - `expect_column_values_to_be_between(column, min_value=None, max_value=None, mostly=1.0)`
  - `expect_column_values_to_be_in_set(column, value_set, mostly=1.0)`
  - `expect_column_values_to_not_be_in_set(column, value_set, mostly=1.0)`
  - `expect_column_values_to_match_regex(column, regex, mostly=1.0)`
  - `expect_column_value_lengths_to_be_between(column, min_value=None, max_value=None, mostly=1.0)`
  - `expect_column_values_to_be_of_type(column, type_, mostly=1.0)`
- **Column Pair Map (Row-level):**
  - `expect_column_pair_values_a_to_be_greater_than_b(column_A, column_B, or_equal=False, mostly=1.0)`
- **Column Aggregate:**
  - `expect_column_max_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_min_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_mean_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_stdev_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_median_to_be_between(column, min_value=None, max_value=None)`

*Powered by Ibis deferred expressions, new expectations can be quickly created via the `@register_expectation` decorator pattern.*
