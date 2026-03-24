import pytest
from dqe.integrations.airflow import DQEValidateOperator, HAS_AIRFLOW
from dqe.integrations.dagster import build_dqe_validate_op, HAS_DAGSTER
from dqe.integrations.prefect import dqe_validate_task, HAS_PREFECT

def test_airflow_import():
    if not HAS_AIRFLOW:
        with pytest.raises(ImportError):
            op = DQEValidateOperator(suite_path="x", primary_data_source_name="y", primary_table_name="z", task_id="test")

def test_dagster_import():
    if not HAS_DAGSTER:
        with pytest.raises(ImportError):
            build_dqe_validate_op(suite_path="x", primary_data_source_name="y", primary_table_name="z")

def test_prefect_import():
    if not HAS_PREFECT:
        with pytest.raises(ImportError):
            dqe_validate_task("x", "y", "z")
