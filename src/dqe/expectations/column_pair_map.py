import ibis
from dqe.core.validator import register_expectation

@register_expectation("expect_column_pair_values_a_to_be_greater_than_b")
def expect_column_pair_values_a_to_be_greater_than_b(table: ibis.expr.types.Table, column_A: str, column_B: str, or_equal: bool = False, mostly: float = 1.0, **kwargs):
    col_a = table[column_A]
    col_b = table[column_B]
    
    if or_equal:
        cond = col_a >= col_b
    else:
        cond = col_a > col_b
        
    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "non_null_count": (~(col_a.isnull() | col_b.isnull())).ifelse(1, 0).sum()
    }
    
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        non_null = resolved_metrics["non_null_count"]
        
        if non_null == 0:
            return True, {"column_A": column_A, "column_B": column_B, "or_equal": or_equal, "mostly": mostly}, {"valid_count": 0}
            
        actual_mostly = valid / non_null
        success = actual_mostly >= mostly
        
        return success, {"column_A": column_A, "column_B": column_B, "or_equal": or_equal, "mostly": mostly}, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "non_null_count": int(non_null)
        }
        
    return metrics, resolve
