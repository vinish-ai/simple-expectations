try:
    from prefect import task
    HAS_PREFECT = True
except ImportError:
    def task(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    HAS_PREFECT = False
    
from typing import Dict, Any, Optional
from pathlib import Path
import dqe

@task(name="dqe_validate", log_prints=True)
def dqe_validate_task(
    suite_path: str | Path,
    primary_data_source_name: str,
    primary_table_name: str,
    data_sources: Optional[Dict[str, Dict[str, Any]]] = None
) -> dict:
    """
    A lightweight, stateless Prefect hook implementation mapping DQE expectation graphs.
    """
    if not HAS_PREFECT:
        raise ImportError("prefect is not natively installed in the module workspace. Please install it to correctly map dqe_validate_task instances.")
        
    suite = dqe.ExpectationSuite.from_yaml(suite_path)
    
    ctx = dqe.Context()
    ctx.add_data_source_from_suite(suite)
    
    for ds_name, config in (data_sources or {}).items():
        ctx.add_data_source(ds_name, **config)
        
    table = ctx.get_table(primary_data_source_name, primary_table_name)
    
    results = ctx.validate(table, suite)
    
    if not results.success:
        failed = results.statistics['unsuccessful_expectations']
        raise RuntimeError(f"Prefect Engine Circuit Breaker TRIPPED: data quality anomaly detected! {failed} specific DQE assertion metrics failed context evaluation limits.")
        
    return results.model_dump()
