import ibis
from simple_expectations.core.validator import register_expectation

@register_expectation("expect_column_to_exist")
def expect_column_to_exist(table: ibis.expr.types.Table, column: str, **kwargs):
    
    def resolve(_):
        success = column in table.columns
        return success, {"column": column}, {"column_exists": success}
        
    return {}, resolve

@register_expectation("expect_column_values_to_be_unique")
def expect_column_values_to_be_unique(table: ibis.expr.types.Table, column: str, **kwargs):
    col = table[column]
    
    metrics = {
        "non_null_count": (~col.isnull()).ifelse(1, 0).sum(),
        "distinct_count": col.nunique()
    }
    
    def resolve(resolved_metrics: dict):
        non_null_count = resolved_metrics["non_null_count"]
        distinct_count = resolved_metrics["distinct_count"]
        
        success = distinct_count == non_null_count
        
        return success, {"column": column}, {
            "distinct_count": int(distinct_count),
            "non_null_count": int(non_null_count)
        }
        
    return metrics, resolve
