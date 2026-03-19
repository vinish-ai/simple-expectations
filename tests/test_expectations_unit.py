import pytest
import ibis
import pandas as pd
from simple_expectations.expectations.column_map import expect_column_values_to_be_null, expect_column_values_to_not_be_null
from simple_expectations.expectations.table_structure import expect_table_columns_to_match_set

@pytest.fixture
def dummy_table():
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    con = ibis.duckdb.connect()
    con.create_table("dummy", ibis.memtable(df))
    return con.table("dummy")

def test_resolve_expect_column_values_to_be_null(dummy_table):
    # Retrieve the resolve_fn
    metrics, resolve_fn = expect_column_values_to_be_null(dummy_table, "id", mostly=0.5)
    
    # Mock resolved metrics assuming 1 null out of 2 total rows
    resolved_metrics = {
        "null_count": 1,
        "total_count": 2
    }
    
    success, kwargs, observed = resolve_fn(resolved_metrics)
    assert success is True
    assert observed["actual_mostly"] == 0.5
    
    # Mock 0 nulls out of 2 => fails mostly=0.5
    resolved_metrics_fail = {
        "null_count": 0,
        "total_count": 2
    }
    success_fail, kwargs_fail, observed_fail = resolve_fn(resolved_metrics_fail)
    assert success_fail is False

def test_resolve_expect_table_columns_to_match_set(dummy_table):
    metrics, resolve_fn = expect_table_columns_to_match_set(dummy_table, column_set=["id", "name"], exact_match=True)
    
    success, kwargs, observed = resolve_fn({})
    assert success is True
    
    metrics_fail, resolve_fn_fail = expect_table_columns_to_match_set(dummy_table, column_set=["id"], exact_match=True)
    success_fail, kwargs_fail, observed_fail = resolve_fn_fail({})
    assert success_fail is False
    
    metrics_sub, resolve_fn_sub = expect_table_columns_to_match_set(dummy_table, column_set=["id"], exact_match=False)
    success_sub, kwargs_sub, observed_sub = resolve_fn_sub({})
    assert success_sub is True
