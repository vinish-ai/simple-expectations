import pytest
import ibis
import pandas as pd
from unittest.mock import patch
from dqe import DatabaseExporter, WebhookExporter
from dqe.core.models import ExpectationSuiteValidationResult

def test_database_exporter():
    con = ibis.duckdb.connect()
    
    result1 = ExpectationSuiteValidationResult(
        suite_name="test_suite",
        success=True,
        results=[],
        statistics={"evaluated_expectations": 5, "successful_expectations": 5, "unsuccessful_expectations": 0}
    )
    result2 = ExpectationSuiteValidationResult(
        suite_name="test_suite",
        success=False,
        results=[],
        statistics={"evaluated_expectations": 5, "successful_expectations": 4, "unsuccessful_expectations": 1}
    )
    
    exporter = DatabaseExporter(connection=con, table_name="metrics_db")
    
    # Export first run (table creation)
    exporter.export(result1)
    assert "metrics_db" in con.list_tables()
    table = con.table("metrics_db")
    assert table.count().execute() == 1
    
    # Export second run (table append)
    exporter.export(result2)
    assert table.count().execute() == 2
    
    # Check that success values persisted correctly natively over engine
    successes = table.select("success").execute()["success"].tolist()
    assert successes == [True, False]

@patch("urllib.request.urlopen")
def test_webhook_exporter_success_only_on_failure(mock_urlopen):
    result1 = ExpectationSuiteValidationResult(
        suite_name="test_suite",
        success=True,
        results=[],
        statistics={"evaluated_expectations": 5, "successful_expectations": 5, "unsuccessful_expectations": 0}
    )
    
    exporter = WebhookExporter(url="https://dummy/webhook", only_on_failure=True)
    exporter.export(result1)
    
    # Since success=True and only_on_failure=True, no POST should be issued
    mock_urlopen.assert_not_called()

@patch("urllib.request.urlopen")
def test_webhook_exporter_fires_on_failure(mock_urlopen):
    result2 = ExpectationSuiteValidationResult(
        suite_name="test_suite",
        success=False,
        results=[],
        statistics={"evaluated_expectations": 5, "successful_expectations": 4, "unsuccessful_expectations": 1}
    )
    
    exporter = WebhookExporter(url="http://dummy/webhook", only_on_failure=True)
    exporter.export(result2)
    
    # Validates endpoint ping
    mock_urlopen.assert_called_once()

@patch("urllib.request.urlopen")
def test_webhook_exporter_bypasses_timeout_crashes(mock_urlopen):
    import urllib.error
    mock_urlopen.side_effect = urllib.error.URLError("mock timeout connection severed")
    
    result2 = ExpectationSuiteValidationResult(
        suite_name="test_suite",
        success=False,
        results=[],
        statistics={"evaluated_expectations": 5, "successful_expectations": 4, "unsuccessful_expectations": 1}
    )
    
    exporter = WebhookExporter(url="http://dummy/webhook", only_on_failure=True)
    # The application shouldn't crash here even though the POST fails!
    exporter.export(result2)
