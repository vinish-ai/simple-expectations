import ibis
from typing import Optional, Any
from dqe.core.validator import register_expectation

@register_expectation("expect_custom_condition")
def expect_custom_condition(
    table: ibis.expr.types.Table, 
    condition: str, 
    compiler: str = "ibis", 
    mostly: float = 1.0, 
    **kwargs
):
    """
    Evaluates a dynamic expression condition directly against the dataset dynamically.
    """
    if compiler == "ibis":
        # Expose ibis and the table explicitly over '_' to fully evaluate AST expressions synchronously
        context_vars = {
            "_": table,
            "ibis": ibis
        }
        try:
            cond = eval(condition, context_vars)
        except Exception as e:
            raise ValueError(f"Failed to dynamically compile custom Ibis condition: {e}")
    else:
        raise ValueError(f"Compiler '{compiler}' is currently not supported for custom conditions.")

    metrics = {
        "valid_count": cond.ifelse(1, 0).sum(),
        "total_count": table.count()
    }
    
    def resolve(resolved_metrics: dict):
        valid = resolved_metrics["valid_count"]
        total = resolved_metrics["total_count"]
        
        if total == 0:
            return True, {"condition": condition, "compiler": compiler, "mostly": mostly}, {"valid_count": 0}
            
        actual_mostly = valid / total
        success = actual_mostly >= mostly
        
        return success, {
            "condition": condition,
            "compiler": compiler,
            "mostly": mostly
        }, {
            "actual_mostly": float(actual_mostly), 
            "valid_count": int(valid), 
            "total_count": int(total)
        }
        
    return metrics, resolve
