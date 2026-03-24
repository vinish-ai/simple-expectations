import ibis
from dqe.core.validator import register_expectation

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

@register_expectation("expect_table_columns_to_match_set")
def expect_table_columns_to_match_set(table: ibis.expr.types.Table, column_set: list, exact_match: bool = True, **kwargs):
    def resolve(_):
        table_cols = set(table.columns)
        expected_cols = set(column_set)
        
        if exact_match:
            success = table_cols == expected_cols
        else:
            success = expected_cols.issubset(table_cols)
            
        return success, {"column_set": column_set, "exact_match": exact_match}, {
            "observed_columns": list(table_cols)
        }
        
    return {}, resolve

@register_expectation("expect_table_columns_to_match_ordered_list")
def expect_table_columns_to_match_ordered_list(table: ibis.expr.types.Table, column_list: list, **kwargs):
    def resolve(_):
        table_cols = list(table.columns)
        success = table_cols == column_list
        return success, {"column_list": column_list}, {"observed_columns": table_cols}
    return {}, resolve
