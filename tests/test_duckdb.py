import pytest
import simple_expectations as ie
import tempfile
import pandas as pd

@pytest.fixture
def table():
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5], 
        "age": [25, 30, 15, None, 99],
        "status": ["active", "inactive", "active", "pending", "active"],
        "email": ["test@example.com", "user@domain.org", "hello@world.net", "invalid", "valid@email.com"]
    })
    
    context = ie.Context()
    # User does not need to import ibis! Just specify backend="pandas"
    context.add_data_source("test_db", backend="pandas", dictionary={"users": df})
    return context.get_table("test_db", "users")

def test_duckdb_expectations(table):
    context = ie.Context()
    
    suite = ie.ExpectationSuite(
        name="test_suite",
        expectations=[
            ie.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
            ie.BaseExpectation(type="expect_column_values_to_not_be_null", kwargs={"column": "id"}),
            ie.BaseExpectation(type="expect_column_values_to_be_between", kwargs={"column": "age", "min_value": 0, "max_value": 100}),
            ie.BaseExpectation(type="expect_table_row_count_to_be_between", kwargs={"min_value": 1, "max_value": 10}),
            ie.BaseExpectation(type="expect_column_values_to_be_in_set", kwargs={"column": "status", "value_set": ["active", "inactive", "pending"]}),
            ie.BaseExpectation(type="expect_column_values_to_be_unique", kwargs={"column": "id"}),
            # 4 out of 5 match (mostly=0.8)
            ie.BaseExpectation(type="expect_column_values_to_match_regex", kwargs={"column": "email", "regex": r"^[\w\.-]+@[\w\.-]+\.\w+$", "mostly": 0.8}),
            ie.BaseExpectation(type="expect_column_max_to_be_between", kwargs={"column": "age", "min_value": 50, "max_value": 100}),
            ie.BaseExpectation(type="expect_column_min_to_be_between", kwargs={"column": "age", "min_value": 10, "max_value": 20})
        ]
    )
    
    results = context.validate(table, suite)
    
    assert results.success
    assert results.statistics["evaluated_expectations"] == 9
    assert results.statistics["successful_expectations"] == 9
    
    # Let's test failure
    suite.expectations.append(ie.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "non_existent"}))
    results_fail = context.validate(table, suite)
    assert not results_fail.success
