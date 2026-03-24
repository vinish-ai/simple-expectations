from pydantic import BaseModel
from typing import Any, Dict

class BaseExpectation(BaseModel):
    type: str
    kwargs: Dict[str, Any] = {}
