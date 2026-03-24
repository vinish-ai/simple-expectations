try:
    from airflow.models import BaseOperator
    from airflow.exceptions import AirflowException
    HAS_AIRFLOW = True
except ImportError:
    # Minimal mock so library doesn't crash if airflow isn't installed natively
    BaseOperator = object
    AirflowException = Exception
    HAS_AIRFLOW = False

from typing import Dict, Any, Optional
from pathlib import Path

import dqe
from dqe.core.models import ExpectationSuiteValidationResult

class DQEValidateOperator(BaseOperator):
    """
    An Airflow operator that validates a data quality expectation suite natively on the Ibis backend.
    If validation fails, an AirflowException is raised, halting the downstream pipeline dependencies recursively.
    """
    def __init__(
        self,
        suite_path: str | Path,
        primary_data_source_name: str,
        primary_table_name: str,
        data_sources: Optional[Dict[str, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        if not HAS_AIRFLOW:
            raise ImportError("apache-airflow is not installed. Please install it to use DQEValidateOperator.")
            
        self.suite_path = suite_path
        self.primary_data_source_name = primary_data_source_name
        self.primary_table_name = primary_table_name
        self.data_sources = data_sources or {}

    def execute(self, context) -> dict:
        self.log.info(f"Loading suite from {self.suite_path}")
        suite = dqe.ExpectationSuite.from_yaml(self.suite_path)
        
        ctx = dqe.Context()
        # Bind intrinsically declared data sources in Yaml configuration statically first
        ctx.add_data_source_from_suite(suite)
        
        # Sequentially map and override dynamically structured contexts from Operator kwarg dictionary bindings
        for ds_name, config in self.data_sources.items():
            ctx.add_data_source(ds_name, **config)
            
        self.log.info(f"Fetching table '{self.primary_table_name}' bound from '{self.primary_data_source_name}'")
        table = ctx.get_table(self.primary_data_source_name, self.primary_table_name)
        
        self.log.info(f"Evaluating the expectations ruleset consisting of {len(suite.expectations)} logical assertions...")
        results: ExpectationSuiteValidationResult = ctx.validate(table, suite)
        
        if not results.success:
            failed = results.statistics['unsuccessful_expectations']
            raise AirflowException(f"Data Quality Validation Failed: {failed} logical expectations were violated!")
            
        self.log.info("DQE Validation succeeded. Flowing to downstream ops.")
        return results.model_dump()
