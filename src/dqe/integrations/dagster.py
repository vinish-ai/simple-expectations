try:
    from dagster import op, Out, Output, Failure
    HAS_DAGSTER = True
except ImportError:
    HAS_DAGSTER = False
    
from typing import Dict, Any, Optional
from pathlib import Path
import dqe

def build_dqe_validate_op(
    name: str = "dqe_validate_op", 
    suite_path: str | Path = None, 
    primary_data_source_name: str = None, 
    primary_table_name: str = None,
    data_sources: Optional[Dict[str, Dict[str, Any]]] = None
):
    """
    Factory that returns a Dagster @op graph object which asserts a DQE Expectations Suite.
    """
    if not HAS_DAGSTER:
        raise ImportError("dagster is not installed in the python environment. Please install it to use build_dqe_validate_op.")
        
    suite_path_str = str(suite_path)
    sources = data_sources or {}
    
    @op(name=name, out=Out(dict))
    def _dqe_validate_op(context):
        context.log.info(f"Loading YAML Suite Configuration sequentially from: {suite_path_str}")
        suite = dqe.ExpectationSuite.from_yaml(suite_path_str)
        
        ctx = dqe.Context()
        ctx.add_data_source_from_suite(suite)
        
        for ds_name, config in sources.items():
            ctx.add_data_source(ds_name, **config)
            
        table = ctx.get_table(primary_data_source_name, primary_table_name)
        
        context.log.info(f"Evaluating assertion metrics for {len(suite.expectations)} mapped expectations...")
        results = ctx.validate(table, suite)
        
        if not results.success:
            failed = results.statistics['unsuccessful_expectations']
            raise Failure(description=f"DQE Pipeline Circuit Breaker Triggered: {failed} mapping assertions failed validation!")
            
        context.log.info("Data Quality verified logically.")
        return Output(results.model_dump())
        
    return _dqe_validate_op
