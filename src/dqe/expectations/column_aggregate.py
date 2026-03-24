import ibis
from dqe.core.validator import register_expectation

@register_expectation("expect_column_max_to_be_between")
def expect_column_max_to_be_between(table: ibis.expr.types.Table, column: str, min_value: float = None, max_value: float = None, **kwargs):
    col = table[column]
    metrics = {
        "max_val": col.max()
    }
    
    def resolve(resolved_metrics: dict):
        max_val = resolved_metrics["max_val"]
        
        if max_val is None:
            return False, {"column": column, "min_value": min_value, "max_value": max_value}, {"max_val": None}
            
        success = True
        if min_value is not None and max_val < min_value:
            success = False
        if max_value is not None and max_val > max_value:
            success = False
            
        return success, {"column": column, "min_value": min_value, "max_value": max_value}, {"max_val": float(max_val)}
        
    return metrics, resolve

@register_expectation("expect_column_min_to_be_between")
def expect_column_min_to_be_between(table: ibis.expr.types.Table, column: str, min_value: float = None, max_value: float = None, **kwargs):
    col = table[column]
    metrics = {
        "min_val": col.min()
    }
    
    def resolve(resolved_metrics: dict):
        min_val = resolved_metrics["min_val"]
        
        if min_val is None:
            return False, {"column": column, "min_value": min_value, "max_value": max_value}, {"min_val": None}
            
        success = True
        if min_value is not None and min_val < min_value:
            success = False
        if max_value is not None and min_val > max_value:
            success = False
            
        return success, {"column": column, "min_value": min_value, "max_value": max_value}, {"min_val": float(min_val)}
        
    return metrics, resolve

@register_expectation("expect_column_mean_to_be_between")
def expect_column_mean_to_be_between(table: ibis.expr.types.Table, column: str, min_value: float = None, max_value: float = None, **kwargs):
    col = table[column]
    metrics = {
        "mean_val": col.mean()
    }
    
    def resolve(resolved_metrics: dict):
        mean_val = resolved_metrics["mean_val"]
        
        if mean_val is None:
            return False, {"column": column, "min_value": min_value, "max_value": max_value}, {"mean_val": None}
            
        success = True
        if min_value is not None and mean_val < min_value:
            success = False
        if max_value is not None and mean_val > max_value:
            success = False
            
        return success, {"column": column, "min_value": min_value, "max_value": max_value}, {"mean_val": float(mean_val)}
        
    return metrics, resolve

@register_expectation("expect_column_stdev_to_be_between")
def expect_column_stdev_to_be_between(table: ibis.expr.types.Table, column: str, min_value: float = None, max_value: float = None, **kwargs):
    col = table[column]
    metrics = {"stdev_val": col.std()}
    
    def resolve(resolved_metrics: dict):
        stdev_val = resolved_metrics["stdev_val"]
        
        if stdev_val is None:
            return False, {"column": column, "min_value": min_value, "max_value": max_value}, {"stdev_val": None}
            
        success = True
        if min_value is not None and stdev_val < min_value:
            success = False
        if max_value is not None and stdev_val > max_value:
            success = False
            
        return success, {"column": column, "min_value": min_value, "max_value": max_value}, {"stdev_val": float(stdev_val)}
        
    return metrics, resolve

@register_expectation("expect_column_median_to_be_between")
def expect_column_median_to_be_between(table: ibis.expr.types.Table, column: str, min_value: float = None, max_value: float = None, **kwargs):
    col = table[column]
    metrics = {"median_val": col.approx_median()}
    
    def resolve(resolved_metrics: dict):
        median_val = resolved_metrics["median_val"]
        
        if median_val is None:
            return False, {"column": column, "min_value": min_value, "max_value": max_value}, {"median_val": None}
            
        success = True
        if min_value is not None and median_val < min_value:
            success = False
        if max_value is not None and median_val > max_value:
            success = False
            
        return success, {"column": column, "min_value": min_value, "max_value": max_value}, {"median_val": float(median_val)}
        
    return metrics, resolve
