import ssl
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime
import pandas as pd
from typing import Any

from dqe.core.models import ExpectationSuiteValidationResult

logger = logging.getLogger(__name__)

class BaseExporter:
    def export(self, result: ExpectationSuiteValidationResult) -> None:
        raise NotImplementedError

class DatabaseExporter(BaseExporter):
    def __init__(self, connection: Any, table_name: str = "dqe_results"):
        """
        Accepts an established Ibis backend connection and writes metric summaries to a tracking table tracking historical metrics over time.
        """
        self.connection = connection
        self.table_name = table_name

    def export(self, result: ExpectationSuiteValidationResult) -> None:
        row = {
            "suite_name": result.suite_name,
            "success": result.success,
            "evaluated_expectations": result.statistics.get("evaluated_expectations", 0),
            "successful_expectations": result.statistics.get("successful_expectations", 0),
            "unsuccessful_expectations": result.statistics.get("unsuccessful_expectations", 0),
            "executed_at": datetime.now()
        }
        df = pd.DataFrame([row])
        
        # If the tracking table hasn't been instantiated yet, auto-scaffold it
        if self.table_name not in self.connection.list_tables():
            self.connection.create_table(self.table_name, df)
        else:
            self.connection.insert(self.table_name, df)


class WebhookExporter(BaseExporter):
    def __init__(self, url: str, only_on_failure: bool = True, verify_ssl: bool = True):
        self.url = url
        self.only_on_failure = only_on_failure
        self.verify_ssl = verify_ssl

    def export(self, result: ExpectationSuiteValidationResult) -> None:
        if self.only_on_failure and result.success:
            return
            
        payload = {
            "text": f"DQE Validation {'FAILED 🚨' if not result.success else 'SUCCEEDED ✅'} for suite *{result.suite_name}*. "
                    f"({result.statistics.get('unsuccessful_expectations', 0)}/{result.statistics.get('evaluated_expectations', 0)} rules failed)"
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.url, data=data, headers={'Content-Type': 'application/json'})
        
        ctx = ssl.create_default_context()
        if not self.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        
        try:
            urllib.request.urlopen(req, timeout=5, context=ctx)
        except Exception as e:
            logger.warning(
                "DQE WebhookExporter failed to deliver alert to %s: %s", 
                self.url, str(e)
            )
