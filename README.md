# Data Quality Engine

**Data Quality Engine** is a modern, lightweight data validation library inspired by Great Expectations. 
It provides a completely stateless, embeddable, and high-performance validation engine powered by [Ibis](https://ibis-project.org/).

Because all validation rules are compiled into deferred Ibis expressions, `dqe` evaluates checks symmetrically across **20+ Execution Backends**, including:
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
- **Severity Levels**: Classify expectations as `"error"` (blocks the pipeline) or `"warning"` (informational only).
- **Row-Level Diagnostics**: Inspect exactly which rows failed a check with `result_format="BASIC"`.
- **Tagging & Filtering**: Organize expectations with tags and selectively run subsets during validation.

## Installation
```bash
pip install dqe
```

*Note: Depending on your choice of execution environment, you'll need the corresponding client installed (e.g. `pip install duckdb pandas polars`).*

Install backend connector extras:
```bash
pip install dqe[backends-embedded]    # DuckDB, Polars, DataFusion
pip install dqe[backends-docker]      # PostgreSQL, MySQL, ClickHouse connectors
pip install dqe[backends-all]         # Everything
```

## Quick Start (Python API)
You can directly construct everything in pure Python objects. This makes embedding validations into DAG workflows (like Airflow or Dagster) effortless!

```python
import pandas as pd
import dqe

# Create a Context and register your data sources
context = dqe.Context()

# Example: Feed it a pandas dataframe (automatically routed to rapid in-memory DuckDB!)
df = pd.DataFrame({"id": [1, 2, 3], "age": [25, 30, None]})
context.add_data_source("my_db", backend="pandas", dictionary={"users": df})

# Let's say we instead wanted to connect straight to a Postgres or Snowflake warehouse:
# context.add_data_source("my_db", backend="postgres", host=..., database=...)

# Read the table from the Context
table = context.get_table("my_db", "users")

# Build the Expectation Suite
suite = dqe.ExpectationSuite(
    name="users_suite",
    expectations=[
        dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
        dqe.BaseExpectation(
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
import dqe

context = dqe.Context()
suite = dqe.ExpectationSuite.from_yaml("my_validations.yaml")

# Automatically provision the duckdb "primary_warehouse" source defined in the YAML!
context.add_data_source_from_suite(suite)

# Read the table and evaluate
table = context.get_table("primary_warehouse", "users")
results = context.validate(table, suite)

print(results.model_dump_json(indent=2))
```

## Severity Levels

Classify expectations as `"error"` (default) or `"warning"`. Warning-severity failures are reported in results but **do not fail the suite** — ideal for informational checks in production pipelines.

```python
suite = dqe.ExpectationSuite(
    name="production_checks",
    expectations=[
        # Hard failure — blocks the pipeline
        dqe.BaseExpectation(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "id"},
            severity="error"
        ),
        # Soft check — logged but doesn't block
        dqe.BaseExpectation(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "middle_name"},
            severity="warning"
        ),
    ]
)

results = context.validate(table, suite)
# results.success is True even if the warning check fails
print(f"Warnings: {results.statistics['warning_expectations']}")
```

In YAML:
```yaml
expectations:
  - type: "expect_column_values_to_not_be_null"
    severity: "warning"
    kwargs:
      column: "middle_name"
```

## Row-Level Failure Diagnostics

When a check fails, inspect the actual failing rows by setting `result_format="BASIC"` (returns up to 20 sample rows) or `"COMPLETE"` (all failing rows):

```python
results = context.validate(table, suite, result_format="BASIC")

for r in results.results:
    if not r.success and r.unexpected_rows:
        print(f"{r.expectation_type} failed on {len(r.unexpected_rows)} sample rows:")
        for row in r.unexpected_rows:
            print(f"  {row}")
```

## Tagging & Filtering

Organize expectations with tags and selectively run subsets:

```python
suite = dqe.ExpectationSuite(
    name="tagged_suite",
    expectations=[
        dqe.BaseExpectation(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "email"},
            tags=["critical", "pii"]
        ),
        dqe.BaseExpectation(
            type="expect_column_values_to_be_between",
            kwargs={"column": "age", "min_value": 0, "max_value": 150},
            tags=["quality"]
        ),
    ]
)

# Run only critical checks
results = context.validate(table, suite, tags=["critical"])
```

In YAML:
```yaml
expectations:
  - type: "expect_column_values_to_not_be_null"
    tags: ["critical", "pii"]
    kwargs:
      column: "email"
```

## CLI Interface
You can quickly generate boilerplate validation suites and run them directly from the command line:
```bash
# Profile an existing table to automatically generate a baseline validation suite!
dqe profile --backend duckdb --kwargs '{"database": "my_data.db"}' --table users --out baseline.yaml

# Generate a starter my_validations.yaml and run_validations.py script
dqe init

# Validate an existing suite YAML
dqe validate my_validations.yaml
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
- **Cross-Table (Reconciliation):**
  - `expect_column_values_to_exist_in_other_table(column, other_table_name, other_column, other_data_source=None, mostly=1.0)`
  - `expect_table_row_count_to_equal_other_table(other_table_name, other_data_source=None)`
- **Custom Logic Expressions:**
  - `expect_custom_condition(condition, compiler="ibis", mostly=1.0)`
- **Column Aggregate:**
  - `expect_column_max_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_min_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_mean_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_stdev_to_be_between(column, min_value=None, max_value=None)`
  - `expect_column_median_to_be_between(column, min_value=None, max_value=None)`

*Powered by Ibis deferred expressions, new expectations can be quickly created via the `@register_expectation` decorator pattern.*

## Result Exporters

Persist validation results and send alerts on failure:

```python
import ibis
from dqe import DatabaseExporter, WebhookExporter

# Persist to a tracking table
con = ibis.duckdb.connect("metrics.db")
db_exporter = DatabaseExporter(connection=con, table_name="dqe_results")
db_exporter.export(results)

# Send Slack/Teams webhook on failure
webhook = WebhookExporter(url="https://hooks.slack.com/...", only_on_failure=True)
webhook.export(results)
```

## Orchestrator Integrations

Native operators for Airflow, Dagster, and Prefect:

```python
# Airflow
from dqe.integrations.airflow import DQEValidateOperator
task = DQEValidateOperator(
    task_id="validate_users",
    suite_path="my_validations.yaml",
    primary_data_source_name="my_db",
    primary_table_name="users",
)

# Dagster
from dqe.integrations.dagster import build_dqe_validate_op
validate_op = build_dqe_validate_op(
    name="validate_users",
    suite_path="my_validations.yaml",
    primary_data_source_name="my_db",
    primary_table_name="users",
)
```

## Development & Testing

Tests are organized into tiers based on infrastructure requirements:

| Tier | Backends | Requirement |
|------|----------|-------------|
| Embedded | DuckDB, Polars, Pandas, DataFusion, SQLite | `pip install` only |
| Docker | PostgreSQL, MySQL, ClickHouse | `docker compose up` |
| Complex | Trino, RisingWave, PySpark | Docker + extra config |

```bash
# Run embedded tests only (no Docker needed)
bash run_tests.sh --embedded -v

# Start Docker services and run all tests
docker compose up -d
bash tests/wait_for_docker.sh
bash run_tests.sh --all -v
docker compose down

# Test specific backends
PYTHONPATH=src uv run pytest tests/test_backends.py --backend duckdb --backend postgres -v
```

Unavailable backends are automatically skipped — no errors for missing infrastructure.
