from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal

class BaseExpectation(BaseModel):
    type: str
    kwargs: Dict[str, Any] = {}
    severity: Literal["error", "warning"] = "error"
    tags: List[str] = Field(default_factory=list)
