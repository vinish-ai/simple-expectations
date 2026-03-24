import ibis
from typing import Optional, Any
from dqe.core.validator import register_expectation

@register_expectation("expect_column_values_to_exist_in_other_table")
def expect_column_values_to_exist_in_other_table(
    table: ibis.expr.types.Table, 
    column: str, 
    other_table_name: str, 
    other_column: str, 
    other_data_source: Optional[str] = None, 
    mostly: float = 1.0, 
    context: Optional[Any] = None, 
    **kwargs
):
    """
    Asserts that values in the base table column explicitly exist in another remote table column (e.g., Foreign Key check).
    """
    if context is None:
        raise ValueError("Context is required to resolve multi-table expectations.")
        
    if other_data_source is None:
        if len(context._data_sources) == 1:
            other_data_source = list(context._data_sources.keys())[0]
        else:
            raise ValueError("other_data_source must be provided if multiple data sources are registered in the Context.")
            
    other_table = context.get_table(other_data_source, other_table_name)
    
    col = table[column]
    other_col = other_table[other_column]
    
    # Deferred Ibis check for row-by-row existence mapping
    cond = col.isin(other_col)
    
    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "non_null_count": (~col.isnull()).ifelse(1, 0).sum()
    }
    
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        non_null = resolved_metrics["non_null_count"]
        
        if non_null == 0:
            return True, {"column": column, "other_table_name": other_table_name, "other_column": other_column, "mostly": mostly}, {"valid_count": 0}
            
        actual_mostly = valid / non_null
        success = actual_mostly >= mostly
        
        return success, {
            "column": column, 
            "other_table_name": other_table_name, 
            "other_column": other_column, 
            "mostly": mostly
        }, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "non_null_count": int(non_null)
        }
        
    return metrics, resolve

@register_expectation("expect_table_row_count_to_equal_other_table")
def expect_table_row_count_to_equal_other_table(
    table: ibis.expr.types.Table, 
    other_table_name: str, 
    other_data_source: Optional[str] = None, 
    context: Optional[Any] = None, 
    **kwargs
):
    """
    Asserts that the aggregate row count of the base table explicitly matches another table.
    """
    if context is None:
        raise ValueError("Context is required to resolve multi-table expectations.")
        
    if other_data_source is None:
        if len(context._data_sources) == 1:
            other_data_source = list(context._data_sources.keys())[0]
        else:
            raise ValueError("other_data_source must be provided if multiple data sources are registered in the Context.")
            
    other_table = context.get_table(other_data_source, other_table_name)
    
    # Extract scalar synchronously during compile phase since single Ibis query evaluates only upon base table mappings
    expected_count = int(other_table.count().execute())
    
    metrics = {
        "row_count": table.count()
    }
    
    def resolve(resolved_metrics: dict):
        row_count = int(resolved_metrics["row_count"])
        
        success = row_count == expected_count
        
        return success, {
            "other_table_name": other_table_name,
            "other_data_source": other_data_source
        }, {
            "row_count": row_count,
            "other_row_count": expected_count
        }
        
    return metrics, resolve
