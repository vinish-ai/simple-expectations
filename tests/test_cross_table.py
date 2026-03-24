import pytest
import pandas as pd
import dqe
import ibis

def test_cross_table_reconciliation():
    users_df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "primary_team_id": [100, 200, 100]
    })
    
    teams_df = pd.DataFrame({
        "team_id": [100, 200],
        "team_name": ["Red", "Blue"]
    })
    
    context = dqe.Context()
    # Explicitly spawn a DuckDB con so we can register multiple tables on it
    con = ibis.duckdb.connect()
    con.create_table("users", users_df)
    con.create_table("teams", teams_df)
    
    context.add_data_source("primary_db", backend="duckdb", connection=con)
    
    users_table = context.get_table("primary_db", "users")
    
    suite = dqe.ExpectationSuite(
        name="cross_table_suite",
        expectations=[
            dqe.BaseExpectation(
                type="expect_column_values_to_exist_in_other_table",
                kwargs={
                    "column": "primary_team_id",
                    "other_table_name": "teams",
                    "other_column": "team_id"
                }
            ),
            # Check implicit self-referential row count just for syntax
            dqe.BaseExpectation(
                type="expect_table_row_count_to_equal_other_table",
                kwargs={
                    "other_table_name": "users"
                }
            )
        ]
    )
    
    results = context.validate(users_table, suite)
    
    assert results.success is True
    assert len(results.results) == 2
    assert results.results[0].success is True
    assert results.results[1].success is True
    
    # Test failure: add a user with an invalid team
    bad_users_df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Dave"],
        "primary_team_id": [100, 200, 100, 999] # 999 is invalid
    })
    con.create_table("bad_users", bad_users_df)
    bad_users_table = context.get_table("primary_db", "bad_users")
    
    bad_results = context.validate(bad_users_table, suite)
    assert bad_results.success is False
    assert bad_results.results[0].success is False 
