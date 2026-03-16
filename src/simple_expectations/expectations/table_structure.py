import ibis
from simple_expectations.core.validator import register_expectation

@register_expectation("expect_table_row_count_to_be_between")
def expect_table_row_count_to_be_between(table: ibis.expr.types.Table, min_value: int = None, max_value: int = None, **kwargs):
    metrics = {
        "row_count": table.count()
    }
    
    def resolve(resolved_metrics: dict):
        row_count = resolved_metrics["row_count"]
        
        success = True
        if min_value is not None and row_count < min_value:
            success = False
        if max_value is not None and row_count > max_value:
            success = False
            
        return success, {"min_value": min_value, "max_value": max_value}, {"row_count": int(row_count)}
        
    return metrics, resolve
