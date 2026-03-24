import ibis
from dqe.core.validator import register_expectation
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

@register_expectation("expect_column_value_lengths_to_be_between")
def expect_column_value_lengths_to_be_between(table: ibis.expr.types.Table, column: str, min_value: int = None, max_value: int = None, mostly: float = 1.0, **kwargs):
    col = table[column].cast('string')
    length_col = col.length()
    
    cond = ibis.literal(True)
    if min_value is not None:
        cond &= (length_col >= min_value)
    if max_value is not None:
        cond &= (length_col <= max_value)
        
    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "non_null_count": (~table[column].isnull()).ifelse(1, 0).sum()
    }
    
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        non_null = resolved_metrics["non_null_count"]
        
        if non_null == 0:
            return True, {"column": column, "min_value": min_value, "max_value": max_value, "mostly": mostly}, {"valid_count": 0}
            
        actual_mostly = valid / non_null
        success = actual_mostly >= mostly
        
        return success, {"column": column, "min_value": min_value, "max_value": max_value, "mostly": mostly}, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "non_null_count": int(non_null)
        }
        
    return metrics, resolve

@register_expectation("expect_column_values_to_be_of_type")
def expect_column_values_to_be_of_type(table: ibis.expr.types.Table, column: str, type_: str, mostly: float = 1.0, **kwargs):
    # Ibis has `.cast()` which will return null if the cast is invalid for backends that support safe casting
    # However, safe casting across all backends isn't universally identical in Ibis yet without `try_cast`.
    # For MVP we can approximate type checking via string matching on the ibis schema
    # Real type checking requires examining the actual ibis schema. This is a structure expectation on the column!
    
    metrics = {
        "non_null_count": (~table[column].isnull()).ifelse(1, 0).sum(),
        "total_count": table.count()
    }
    
    schema_type = str(table[column].type())
    
    def resolve(resolved_metrics: dict):
        # We perform a generic check against the schema type instead of row-by-row
        # since Ibis enforces strong typing at the column level.
        success = type_.lower() in schema_type.lower()
        
        non_null_count = resolved_metrics.get("non_null_count", 0)
        
        return success, {"column": column, "type_": type_}, {
            "observed_type": schema_type,
            "non_null_count": int(non_null_count)
        }
        
    return metrics, resolve

@register_expectation("expect_column_values_to_not_be_in_set")
def expect_column_values_to_not_be_in_set(table: ibis.expr.types.Table, column: str, value_set: List[str | int | float], mostly: float = 1.0, **kwargs):
    col = table[column]
    cond = ~col.isin(value_set)
    
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

@register_expectation("expect_column_values_to_be_null")
def expect_column_values_to_be_null(table: ibis.expr.types.Table, column: str, mostly: float = 1.0, **kwargs):
    col = table[column]
    
    metrics = {
        "null_count": col.isnull().ifelse(1, 0).sum(),
        "total_count": table.count()
    }
    
    def resolve(resolved_metrics: dict):
        null_count = resolved_metrics["null_count"]
        total = resolved_metrics["total_count"]
        
        if total == 0:
            return True, {"column": column, "mostly": mostly}, {"null_fraction": 0.0}
            
        actual_mostly = null_count / total
        success = actual_mostly >= mostly
        
        return success, {"column": column, "mostly": mostly}, {
            "actual_mostly": float(actual_mostly), 
            "null_count": int(null_count), 
            "total_count": int(total)
        }
        
    return metrics, resolve
