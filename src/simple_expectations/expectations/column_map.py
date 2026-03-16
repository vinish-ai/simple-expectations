import ibis
from simple_expectations.core.validator import register_expectation
from typing import List

@register_expectation("expect_column_values_to_not_be_null")
def expect_column_values_to_not_be_null(table: ibis.expr.types.Table, column: str, mostly: float = 1.0, **kwargs):
    col = table[column]
    
    # 1. Define deferred aggregate metrics
    metrics = {
        "non_null_count": (~col.isnull()).ifelse(1, 0).sum(),
        "total_count": table.count()
    }
    
    # 2. Define how to resolve those executed metrics back into success bool + observed values
    def resolve(resolved_metrics: dict):
        non_null = resolved_metrics["non_null_count"]
        total = resolved_metrics["total_count"]
        
        if total == 0:
            return True, {"column": column, "mostly": mostly}, {"null_fraction": 0.0}
            
        actual_mostly = non_null / total
        success = actual_mostly >= mostly
        
        return success, {"column": column, "mostly": mostly}, {
            "actual_mostly": float(actual_mostly), 
            "non_null_count": int(non_null), 
            "total_count": int(total)
        }
        
    return metrics, resolve

@register_expectation("expect_column_values_to_be_between")
def expect_column_values_to_be_between(table: ibis.expr.types.Table, column: str, min_value: float = None, max_value: float = None, mostly: float = 1.0, **kwargs):
    col = table[column]
    
    cond = ibis.literal(True)
    if min_value is not None:
        cond &= (col >= min_value)
    if max_value is not None:
        cond &= (col <= max_value)
        
    # 1. Define deferred aggregate metrics
    # We must use conditional sum on the BASE table to avoid creating a new relation
    # that cannot be natively aggregated alongside other base table aggregates.
    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "non_null_count": (~col.isnull()).ifelse(1, 0).sum()
    }
    
    # 2. Define resolution callback
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        non_null = resolved_metrics["non_null_count"]
        
        if non_null == 0:
            return True, {"column": column, "min_value": min_value, "max_value": max_value}, {"valid_count": 0}
            
        actual_mostly = valid / non_null
        success = actual_mostly >= mostly
        
        return success, {"column": column, "min_value": min_value, "max_value": max_value, "mostly": mostly}, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "non_null_count": int(non_null)
        }
        
    return metrics, resolve

@register_expectation("expect_column_values_to_be_in_set")
def expect_column_values_to_be_in_set(table: ibis.expr.types.Table, column: str, value_set: List[str | int | float], mostly: float = 1.0, **kwargs):
    col = table[column]
    cond = col.isin(value_set)
    
    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "non_null_count": (~col.isnull()).ifelse(1, 0).sum()
    }
    
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        non_null = resolved_metrics["non_null_count"]
        
        if non_null == 0:
            return True, {"column": column, "mostly": mostly, "value_set": value_set}, {"valid_count": 0}
            
        actual_mostly = valid / non_null
        success = actual_mostly >= mostly
        
        return success, {"column": column, "mostly": mostly, "value_set": value_set}, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "non_null_count": int(non_null)
        }
        
    return metrics, resolve

@register_expectation("expect_column_values_to_match_regex")
def expect_column_values_to_match_regex(table: ibis.expr.types.Table, column: str, regex: str, mostly: float = 1.0, **kwargs):
    col = table[column].cast('string') # coerce safely
    cond = col.re_search(regex)
    
    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "non_null_count": (~table[column].isnull()).ifelse(1, 0).sum() # Need to check raw column for nulls
    }
    
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        non_null = resolved_metrics["non_null_count"]
        
        if non_null == 0:
            return True, {"column": column, "mostly": mostly, "regex": regex}, {"valid_count": 0}
            
        actual_mostly = valid / non_null
        success = actual_mostly >= mostly
        
        return success, {"column": column, "mostly": mostly, "regex": regex}, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "non_null_count": int(non_null)
        }
        
    return metrics, resolve
