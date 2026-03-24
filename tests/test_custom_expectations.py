import pytest
import pandas as pd
import dqe
import ibis

def test_custom_condition_ibis_compiler():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "revenue": [100.0, 250.5, 50.0],
        "cost": [50.0, 200.0, 60.0]
    })
    
    context = dqe.Context()
    con = ibis.duckdb.connect()
    con.create_table("sales", df)
    context.add_data_source("primary_db", backend="duckdb", connection=con)
    
    sales_table = context.get_table("primary_db", "sales")
    
    suite = dqe.ExpectationSuite(
        name="custom_suite",
        expectations=[
            dqe.BaseExpectation(
                type="expect_custom_condition",
                kwargs={
                    "condition": "_.revenue > _.cost", 
                    "mostly": 0.6
                }
            ),
            dqe.BaseExpectation(
                type="expect_custom_condition",
                kwargs={
                    "condition": "_.revenue > _.cost",
                    "mostly": 0.9 
                }
            )
        ]
    )
    
    results = context.validate(sales_table, suite)
    assert results.success is False
    assert len(results.results) == 2
    
    assert results.results[0].success is True
    assert results.results[0].observed_value["valid_count"] == 2
    
    assert results.results[1].success is False
