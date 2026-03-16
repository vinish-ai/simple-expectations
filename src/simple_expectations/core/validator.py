import ibis
from typing import List, Dict, Callable

from simple_expectations.core.suite import ExpectationSuite
from simple_expectations.core.models import ExpectationSuiteValidationResult, ExpectationValidationResult
from simple_expectations.core.expectation import BaseExpectation

# A simple registry to hold evaluation functions for each expectation type
_EXPECTATION_REGISTRY: Dict[str, Callable] = {}

def register_expectation(name: str):
    def decorator(fn: Callable):
        _EXPECTATION_REGISTRY[name] = fn
        return fn
    return decorator

class Validator:
    def __init__(self, table: ibis.expr.types.Table):
        self.table = table

    def validate(self, suite: ExpectationSuite) -> ExpectationSuiteValidationResult:
        results: List[ExpectationValidationResult] = []
        
        # Phase 1: Collect metrics from all expectations
        all_metrics = {}
        # Keep track of the resolution functions and kwargs for later mapping
        expectation_resolvers = []
        
        for i, exp in enumerate(suite.expectations):
            if exp.type not in _EXPECTATION_REGISTRY:
                results.append(ExpectationValidationResult(
                    expectation_type=exp.type,
                    success=False,
                    kwargs=exp.kwargs,
                    exception_info={"error": f"Expectation type '{exp.type}' not registered."}
                ))
                continue
                
            eval_fn = _EXPECTATION_REGISTRY[exp.type]
            try:
                # evaluate function now returns (metrics_dict, resolve_fn)
                metrics, resolve_fn = eval_fn(self.table, **exp.kwargs)
                
                # Prefix metrics to prevent namespace collisions between expectations
                prefixed_metrics = {
                    f"exp_{i}_{k}": v for k, v in metrics.items()
                }
                all_metrics.update(prefixed_metrics)
                
                expectation_resolvers.append({
                    "exp": exp,
                    "index": i,
                    "resolve_fn": resolve_fn,
                    "metric_keys": list(metrics.keys())
                })
                
            except Exception as e:
                results.append(ExpectationValidationResult(
                    expectation_type=exp.type,
                    success=False,
                    kwargs=exp.kwargs,
                    exception_info={"error": str(e)}
                ))
                
        # Phase 2: Execute all deferred queries in a single SQL pass!
        evaluated_metrics = {}
        if all_metrics:
            try:
                evaluated_metrics = self.table.aggregate(**all_metrics).execute().to_dict('records')[0]
            except Exception as e:
                # If the massive aggregate query fails, we must hard fail the remaining expectations
                for r in expectation_resolvers:
                    results.append(ExpectationValidationResult(
                        expectation_type=r["exp"].type,
                        success=False,
                        kwargs=r["exp"].kwargs,
                        exception_info={"error": f"Backend aggregation execution failed: {str(e)}"}
                    ))
                expectation_resolvers = [] # Skip Phase 3
                
        # Phase 3: Map backend resolved metrics back into individual expectation success conditions
        for r in expectation_resolvers:
            exp = r["exp"]
            i = r["index"]
            resolve_fn = r["resolve_fn"]
            
            # Extract just the metrics this expectation asked for
            local_metrics = {
                k: evaluated_metrics.get(f"exp_{i}_{k}") 
                for k in r["metric_keys"]
            }
            
            try:
                success, kwargs, observed = resolve_fn(local_metrics)
                results.append(ExpectationValidationResult(
                    expectation_type=exp.type,
                    success=success,
                    kwargs=kwargs,
                    observed_value=observed
                ))
            except Exception as e:
                results.append(ExpectationValidationResult(
                    expectation_type=exp.type,
                    success=False,
                    kwargs=exp.kwargs,
                    exception_info={"error": str(e)}
                ))

        success = all(r.success for r in results) if results else True
        return ExpectationSuiteValidationResult(
            suite_name=suite.name,
            success=success,
            results=results,
            statistics={
                "evaluated_expectations": len(suite.expectations),
                "successful_expectations": sum(1 for r in results if r.success),
                "unsuccessful_expectations": sum(1 for r in results if not r.success)
            }
        )
