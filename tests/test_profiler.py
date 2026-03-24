import pytest
import pandas as pd
import dqe
import ibis

def test_profiler_generates_suite():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", None],
        "age": [25, 30, 35]
    })
    
    con = ibis.duckdb.connect()
    table = con.create_table("users", df)
    
    suite = dqe.Profiler.profile_table(table, suite_name="test_profile")
    
    assert suite.name == "test_profile"
    
    expectations = suite.expectations
    assert len(expectations) == 11
    
    types = [e.type for e in expectations]
    assert "expect_table_row_count_to_be_between" in types
    assert "expect_column_to_exist" in types
    assert "expect_column_values_to_be_of_type" in types
    assert "expect_column_values_to_not_be_null" in types
    assert "expect_column_values_to_be_between" in types
    
    age_bounds = next(e for e in expectations if e.type == "expect_column_values_to_be_between" and e.kwargs["column"] == "age")
    assert age_bounds.kwargs["min_value"] == 25
    assert age_bounds.kwargs["max_value"] == 35

    id_not_null = next((e for e in expectations if e.type == "expect_column_values_to_not_be_null" and e.kwargs["column"] == "id"), None)
    assert id_not_null is not None

def test_profiler_suite_validates_successfully():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", None],
        "age": [25, 30, 35]
    })
    
    con = ibis.duckdb.connect()
    table = con.create_table("users", df)
    
    suite = dqe.Profiler.profile_table(table, suite_name="test_profile")
    
    context = dqe.Context()
    context.add_data_source("test_db", backend="duckdb", connection=con)
    table_from_ctx = context.get_table("test_db", "users")
    
    results = context.validate(table_from_ctx, suite)
    
    assert results.success is True
    for result in results.results:
        assert result.success is True

def test_profiler_empty_table():
    df = pd.DataFrame({"id": pd.Series(dtype='int64')})
    
    con = ibis.duckdb.connect()
    table = con.create_table("users", df)
    
    suite = dqe.Profiler.profile_table(table, suite_name="empty_table_test")
    
    assert len(suite.expectations) == 1
    assert suite.expectations[0].type == "expect_table_row_count_to_be_between"
    assert suite.expectations[0].kwargs["min_value"] == 0
