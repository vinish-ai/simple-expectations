import pytest
import pandas as pd
import dqe


@pytest.fixture
def context_and_table():
    """Shared fixture: a pandas-backed Context with a 'users' table."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "age": [25.0, 30.0, 15.0, None, 150.0],  # 150 is out of [0,100] range
        "status": ["active", "inactive", "active", "pending", "INVALID"],
        "email": ["a@b.com", "c@d.org", "e@f.net", "invalid", "g@h.io"]
    })
    context = dqe.Context()
    context.add_data_source("db", backend="pandas", dictionary={"users": df})
    table = context.get_table("db", "users")
    return context, table


# ============================================================
# Phase 1: Bug fix verification
# ============================================================

def test_backend_tuple_membership():
    """Verify 'pandas' backend works correctly after the tuple fix."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    ctx = dqe.Context()
    ctx.add_data_source("test", backend="pandas", dictionary={"t": df})
    table = ctx.get_table("test", "t")
    suite = dqe.ExpectationSuite(
        name="tuple_test",
        expectations=[dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "x"})]
    )
    result = ctx.validate(table, suite)
    assert result.success


# ============================================================
# Phase 2: Severity levels
# ============================================================

def test_warning_severity_does_not_fail_suite(context_and_table):
    """A failing warning-severity expectation should NOT fail the suite."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="severity_test",
        expectations=[
            # This will PASS
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
            # This will FAIL (age has a null), but severity=warning so suite should still pass
            dqe.BaseExpectation(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "age"},
                severity="warning"
            ),
        ]
    )
    result = context.validate(table, suite)
    assert result.success is True
    assert result.statistics["warning_expectations"] == 1


def test_error_severity_fails_suite(context_and_table):
    """A failing error-severity expectation should fail the suite."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="severity_error_test",
        expectations=[
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
            # This FAILS with severity=error (default)
            dqe.BaseExpectation(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "age"},
                severity="error"
            ),
        ]
    )
    result = context.validate(table, suite)
    assert result.success is False


def test_severity_propagated_to_results(context_and_table):
    """Each result should carry the severity from its expectation."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="severity_prop_test",
        expectations=[
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}, severity="error"),
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "age"}, severity="warning"),
        ]
    )
    result = context.validate(table, suite)
    assert result.results[0].severity == "error"
    assert result.results[1].severity == "warning"


def test_all_warnings_suite_passes(context_and_table):
    """If ALL expectations are warnings and some fail, suite should still pass."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="all_warnings",
        expectations=[
            dqe.BaseExpectation(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "age"},
                severity="warning"
            ),
            dqe.BaseExpectation(
                type="expect_column_values_to_be_between",
                kwargs={"column": "age", "min_value": 0, "max_value": 100},
                severity="warning"
            ),
        ]
    )
    result = context.validate(table, suite)
    assert result.success is True


# ============================================================
# Phase 3: Row-level diagnostics
# ============================================================

def test_result_format_basic_returns_failing_rows(context_and_table):
    """result_format='BASIC' should populate unexpected_rows for failed expectations."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="diag_test",
        expectations=[
            # age=150 is out of [0,100] range -> 1 failing row
            dqe.BaseExpectation(
                type="expect_column_values_to_be_between",
                kwargs={"column": "age", "min_value": 0, "max_value": 100}
            ),
        ]
    )
    result = context.validate(table, suite, result_format="BASIC")
    assert result.success is False
    
    failed = result.results[0]
    assert failed.unexpected_rows is not None
    assert len(failed.unexpected_rows) >= 1
    # The failing row should have age=150
    failing_ages = [r["age"] for r in failed.unexpected_rows]
    assert 150.0 in failing_ages


def test_result_format_summary_no_rows(context_and_table):
    """result_format='SUMMARY' (default) should NOT populate unexpected_rows."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="summary_test",
        expectations=[
            dqe.BaseExpectation(
                type="expect_column_values_to_be_between",
                kwargs={"column": "age", "min_value": 0, "max_value": 100}
            ),
        ]
    )
    result = context.validate(table, suite)
    assert result.results[0].unexpected_rows is None


def test_passing_expectations_no_rows(context_and_table):
    """Even with BASIC format, passing expectations should not have unexpected_rows."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="pass_test",
        expectations=[
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}),
        ]
    )
    result = context.validate(table, suite, result_format="BASIC")
    assert result.results[0].unexpected_rows is None


# ============================================================
# Phase 4: Tagging & filtering
# ============================================================

def test_tag_filtering(context_and_table):
    """Only expectations matching the given tags should be evaluated."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="tag_test",
        expectations=[
            dqe.BaseExpectation(
                type="expect_column_to_exist",
                kwargs={"column": "id"},
                tags=["critical"]
            ),
            dqe.BaseExpectation(
                type="expect_column_to_exist",
                kwargs={"column": "non_existent"},
                tags=["optional"]
            ),
        ]
    )
    # Only run "critical" tagged expectations — the "optional" one (which would fail) is skipped
    result = context.validate(table, suite, tags=["critical"])
    assert result.success is True
    assert result.statistics["evaluated_expectations"] == 1


def test_no_tags_runs_all(context_and_table):
    """When no tags are specified, all expectations should be evaluated."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="no_tag_test",
        expectations=[
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}, tags=["a"]),
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "age"}, tags=["b"]),
        ]
    )
    result = context.validate(table, suite)
    assert result.statistics["evaluated_expectations"] == 2


def test_multiple_tags_union(context_and_table):
    """Providing multiple tags should be a union — run expectations matching ANY of the tags."""
    context, table = context_and_table
    suite = dqe.ExpectationSuite(
        name="multi_tag_test",
        expectations=[
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "id"}, tags=["a"]),
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "age"}, tags=["b"]),
            dqe.BaseExpectation(type="expect_column_to_exist", kwargs={"column": "status"}, tags=["c"]),
        ]
    )
    result = context.validate(table, suite, tags=["a", "b"])
    assert result.statistics["evaluated_expectations"] == 2


def test_tags_in_yaml_round_trip():
    """Tags and severity should survive YAML serialization."""
    suite = dqe.ExpectationSuite(
        name="yaml_rt",
        expectations=[
            dqe.BaseExpectation(
                type="expect_column_to_exist",
                kwargs={"column": "id"},
                severity="warning",
                tags=["critical", "pii"]
            ),
        ]
    )
    yaml_str = suite.to_yaml()
    assert "warning" in yaml_str
    assert "critical" in yaml_str
    
    # Round-trip: parse back from YAML
    import tempfile, os
    path = os.path.join(tempfile.gettempdir(), "test_rt.yaml")
    suite.to_yaml(path)
    loaded = dqe.ExpectationSuite.from_yaml(path)
    assert loaded.expectations[0].severity == "warning"
    assert "critical" in loaded.expectations[0].tags
    os.unlink(path)
