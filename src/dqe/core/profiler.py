import ibis
from dqe.core.suite import ExpectationSuite
from dqe.core.expectation import BaseExpectation

class Profiler:
    """
    Automatically profiles an Ibis table and generates a baseline ExpectationSuite.
    """
    
    @classmethod
    def profile_table(cls, table: ibis.expr.types.Table, suite_name: str = "auto_generated_suite") -> ExpectationSuite:
        expectations = []
        
        # 1. Expect Table Row Count
        row_count = int(table.count().execute())
        expectations.append(
            BaseExpectation(
                type="expect_table_row_count_to_be_between",
                kwargs={"min_value": row_count, "max_value": row_count}
            )
        )
        
        if row_count == 0:
            return ExpectationSuite(name=suite_name, expectations=expectations)
            
        # 2. Extract column metrics in a single pass using deferred Ibis aggregates
        aggs = []
        columns = table.columns
        for col_name in columns:
            col = table[col_name]
            aggs.append((~col.isnull()).ifelse(1, 0).sum().name(f"{col_name}_non_null_count"))
            if col.type().is_numeric() or col.type().is_timestamp() or col.type().is_date():
                aggs.append(col.min().name(f"{col_name}_min"))
                aggs.append(col.max().name(f"{col_name}_max"))
                
        metrics = table.aggregate(aggs).execute().iloc[0]
        
        # 3. Generate column-level expectations from the fetched metrics
        for col_name in columns:
            col_type = str(table[col_name].type())
            
            # A. Column must exist
            expectations.append(
                BaseExpectation(
                    type="expect_column_to_exist",
                    kwargs={"column": col_name}
                )
            )
            
            # B. Column Type
            expectations.append(
                BaseExpectation(
                    type="expect_column_values_to_be_of_type",
                    kwargs={"column": col_name, "type_": col_type}
                )
            )
            
            # C. Not Null if fully populated
            non_null = int(metrics.get(f"{col_name}_non_null_count", 0))
            if non_null == row_count:
                expectations.append(
                    BaseExpectation(
                        type="expect_column_values_to_not_be_null",
                        kwargs={"column": col_name}
                    )
                )
                
            # D. Min/Max bounds if numeric/temporal
            if getattr(table[col_name].type(), "is_numeric", lambda: False)() or getattr(table[col_name].type(), "is_timestamp", lambda: False)() or getattr(table[col_name].type(), "is_date", lambda: False)():
                min_val = metrics.get(f"{col_name}_min")
                max_val = metrics.get(f"{col_name}_max")
                
                # Convert pandas/numpy types internally to native Python types for clean pydantic/yaml serialization
                if min_val is not None:
                    if hasattr(min_val, "item"):
                        min_val = min_val.item()
                    if hasattr(max_val, "item"):
                        max_val = max_val.item()
                        
                expectations.append(
                    BaseExpectation(
                        type="expect_column_values_to_be_between",
                        kwargs={
                            "column": col_name,
                            "min_value": min_val,
                            "max_value": max_val
                        }
                    )
                )
                
        return ExpectationSuite(name=suite_name, expectations=expectations)
