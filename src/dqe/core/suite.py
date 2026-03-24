from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import yaml
from pathlib import Path

from dqe.core.expectation import BaseExpectation

class DataSourceProfile(BaseModel):
    name: str
    backend: str
    table_name: str | None = None
    kwargs: Dict[str, Any] = Field(default_factory=dict)

class ExpectationSuite(BaseModel):
    name: str
    expectations: List[BaseExpectation]
    data_sources: Optional[List[DataSourceProfile]] = None

    @classmethod
    def from_yaml(cls, filepath: str | Path) -> "ExpectationSuite":
        with open(filepath, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def to_yaml(self, filepath: Optional[str | Path] = None) -> Optional[str]:
        data = yaml.dump(self.model_dump(exclude_none=True), default_flow_style=False)
        if filepath:
            with open(filepath, "w") as f:
                f.write(data)
        else:
            return data
