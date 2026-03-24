import pytest
import dqe as ie
import pandas as pd

@pytest.fixture(params=["pandas", "polars", "pyspark"])
def table(request):
    backend = request.param
    
    # Base dataset
    data = {
        "id": [1, 2, 3, 4, 5], 
        "age": [25.0, 30.0, 15.0, None, 99.0], # Use floats for null compatibility across engines
        "status": ["active", "inactive", "active", "pending", "active"],
        "email": ["test@example.com", "user@domain.org", "hello@world.net", "invalid", "valid@email.com"]
    }
    
    context = ie.Context()
    
    if backend == "pandas":
        df = pd.DataFrame(data)
        context.add_data_source("test_db", backend="pandas", dictionary={"users": df})
    elif backend == "polars":
        import polars as pl
        import ibis
        df = pl.DataFrame(data)
        con = ibis.polars.connect()
        con.create_table("users", df)
        context.add_data_source("test_db", backend="polars", connection=con)
    elif backend == "pyspark":
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("test").getOrCreate()
            df = pd.DataFrame(data)
            spark_df = spark.createDataFrame(df)
            spark_df.createOrReplaceTempView("users")
            context.add_data_source("test_db", backend="pyspark")
        except Exception as e:
            pytest.skip(f"PySpark not available on system, skipping: {e}")
            
    return context.get_table("test_db", "users")

def test_backend_expectations(table):
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
            ie.BaseExpectation(type="expect_column_min_to_be_between", kwargs={"column": "age", "min_value": 10, "max_value": 20}),
            ie.BaseExpectation(type="expect_column_value_lengths_to_be_between", kwargs={"column": "status", "min_value": 6, "max_value": 10}),
            ie.BaseExpectation(type="expect_column_values_to_be_of_type", kwargs={"column": "age", "type_": "float"}),
            ie.BaseExpectation(type="expect_column_values_to_not_be_in_set", kwargs={"column": "status", "value_set": ["deleted", "banned"]})
        ]
    )
    
    results = context.validate(table, suite)
    
    assert results.success
    assert results.statistics["evaluated_expectations"] == 12
    assert results.statistics["successful_expectations"] == 12
    
    # Let's test failure
    suite.expectations.append(ie.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "non_existent"}))
    results_fail = context.validate(table, suite)
    assert not results_fail.success
