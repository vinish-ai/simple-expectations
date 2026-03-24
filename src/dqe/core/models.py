from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, Optional, List

class ExpectationValidationResult(BaseModel):
    expectation_type: str
    success: bool
    kwargs: Dict[str, Any]
    observed_value: Optional[Any] = None
    exception_info: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="allow")

class ExpectationSuiteValidationResult(BaseModel):
    suite_name: str
    success: bool
    results: List[ExpectationValidationResult]
    statistics: Dict[str, int]
