"""
Multi-backend integration tests for DQE.

Uses the `backend_table` fixture from conftest.py which parametrizes
across all available Ibis backends (11 backends, 3 tiers).

Run specific tiers:
    pytest tests/test_backends.py --tier embedded        # no Docker needed
    pytest tests/test_backends.py --tier docker           # requires docker compose up
    pytest tests/test_backends.py --backend duckdb --backend polars  # specific backends
"""

import pytest
import dqe


class TestCoreExpectations:
    """Validate that all core expectations work correctly across backends."""

    def test_column_existence(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"existence_{backend}",
            expectations=[
                dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
                dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "age"}),
                dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "status"}),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Column existence failed: {results.model_dump_json()}"

    def test_not_null(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"not_null_{backend}",
            expectations=[
                # 'id' is fully populated
                dqe.BaseExpectation(
                    type="expect_column_values_to_not_be_null",
                    kwargs={"column": "id"}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Not-null check failed"

    def test_between(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"between_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_values_to_be_between",
                    kwargs={"column": "age", "min_value": 0, "max_value": 100}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Between check failed"

    def test_in_set(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"in_set_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_values_to_be_in_set",
                    kwargs={"column": "status", "value_set": ["active", "inactive", "pending"]}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] In-set check failed"

    def test_not_in_set(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"not_in_set_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_values_to_not_be_in_set",
                    kwargs={"column": "status", "value_set": ["deleted", "banned"]}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Not-in-set check failed"

    def test_row_count(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"row_count_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_table_row_count_to_be_between",
                    kwargs={"min_value": 1, "max_value": 10}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Row count check failed"

    def test_uniqueness(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"unique_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_values_to_be_unique",
                    kwargs={"column": "id"}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Uniqueness check failed"


class TestAggregateExpectations:
    """Validate aggregate expectations across backends."""

    def test_column_max(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"max_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_max_to_be_between",
                    kwargs={"column": "age", "min_value": 50, "max_value": 100}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Max check failed"

    def test_column_min(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"min_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_min_to_be_between",
                    kwargs={"column": "age", "min_value": 10, "max_value": 20}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Min check failed"

    def test_column_mean(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"mean_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_mean_to_be_between",
                    kwargs={"column": "age", "min_value": 30, "max_value": 50}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Mean check failed"


class TestRegexExpectations:
    """Regex pushdown varies across backends — test it specifically."""

    def test_regex_match(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"regex_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_values_to_match_regex",
                    kwargs={
                        "column": "email",
                        "regex": r"^[\w\.\-]+@[\w\.\-]+\.\w+$",
                        "mostly": 0.8
                    }
                ),
            ]
        )
        results = context.validate(table, suite)
        assert results.success, f"[{backend}] Regex check failed"


class TestStringLengthExpectations:
    """String length expectations across backends."""

    def test_value_lengths(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"lengths_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_value_lengths_to_be_between",
                    kwargs={"column": "status", "min_value": 4, "max_value": 10}
                ),
            ]
        )
        results = context.validate(table, suite)
        # "active"=6, "inactive"=8, "pending"=7 -> all within [4,10]
        assert results.success, f"[{backend}] String length check failed"


class TestFailureDetection:
    """Ensure failures are correctly detected across all backends."""

    def test_nonexistent_column_fails(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"fail_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_to_exist",
                    kwargs={"column": "nonexistent_column"}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert not results.success, f"[{backend}] Should have detected missing column"

    def test_null_column_fails_strict(self, backend_table):
        context, table, backend = backend_table
        # 'age' has 1 null out of 5 rows -> fails with mostly=1.0
        suite = dqe.ExpectationSuite(
            name=f"null_fail_{backend}",
            expectations=[
                dqe.BaseExpectation(
                    type="expect_column_values_to_not_be_null",
                    kwargs={"column": "age", "mostly": 1.0}
                ),
            ]
        )
        results = context.validate(table, suite)
        assert not results.success, f"[{backend}] Should have detected nulls"


class TestFullSuite:
    """Run the full validation suite across backends (matches original test_backends.py coverage)."""

    def test_comprehensive_suite(self, backend_table):
        context, table, backend = backend_table
        suite = dqe.ExpectationSuite(
            name=f"comprehensive_{backend}",
            expectations=[
                dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
                dqe.BaseExpectation(type="expect_column_values_to_not_be_null", kwargs={"column": "id"}),
                dqe.BaseExpectation(type="expect_column_values_to_be_between", kwargs={"column": "age", "min_value": 0, "max_value": 100}),
                dqe.BaseExpectation(type="expect_table_row_count_to_be_between", kwargs={"min_value": 1, "max_value": 10}),
                dqe.BaseExpectation(type="expect_column_values_to_be_in_set", kwargs={"column": "status", "value_set": ["active", "inactive", "pending"]}),
                dqe.BaseExpectation(type="expect_column_values_to_be_unique", kwargs={"column": "id"}),
                dqe.BaseExpectation(type="expect_column_values_to_match_regex", kwargs={"column": "email", "regex": r"^[\w\.\-]+@[\w\.\-]+\.\w+$", "mostly": 0.8}),
                dqe.BaseExpectation(type="expect_column_max_to_be_between", kwargs={"column": "age", "min_value": 50, "max_value": 100}),
                dqe.BaseExpectation(type="expect_column_min_to_be_between", kwargs={"column": "age", "min_value": 10, "max_value": 20}),
                dqe.BaseExpectation(type="expect_column_value_lengths_to_be_between", kwargs={"column": "status", "min_value": 4, "max_value": 10}),
                dqe.BaseExpectation(type="expect_column_values_to_not_be_in_set", kwargs={"column": "status", "value_set": ["deleted", "banned"]}),
            ]
        )
        results = context.validate(table, suite)

        assert results.success, f"[{backend}] Comprehensive suite failed: {[r.expectation_type for r in results.results if not r.success]}"
        assert results.statistics["evaluated_expectations"] == 11
        assert results.statistics["successful_expectations"] == 11
