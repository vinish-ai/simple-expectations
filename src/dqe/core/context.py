import ibis
from typing import Any, Dict, Optional

from dqe.core.suite import ExpectationSuite
from dqe.core.models import ExpectationSuiteValidationResult
from dqe.core.validator import Validator

class Context:
    def __init__(self):
        self._data_sources: Dict[str, Any] = {}

    def add_data_source_from_suite(self, suite: ExpectationSuite) -> None:
        """Register all data sources defined intrinsically within an ExpectationSuite."""
        if not suite.data_sources:
            return
            
        for source in suite.data_sources:
            self.add_data_source(name=source.name, backend=source.backend, **source.kwargs)

    def add_data_source(self, name: str, backend: str, connection: Optional[Any] = None, **kwargs) -> None:
        """
        Register a data source without needing to know about ibis primitives.
        
        Args:
            name: The internal name for this data source.
            backend: The target backend (e.g., "duckdb", "pandas", "polars", "snowflake").
            connection: An optional existing connection object (if the user already has one).
            **kwargs: Connection parameters passed to the backend.
        """
        if connection is not None:
            self._data_sources[name] = connection
            return

        # Ibis 9.0+ removed native pandas/polars backends. We transparently 
        # route these to an in-memory duckdb connection for better performance.
        if backend in ("pandas"):
            con = ibis.duckdb.connect()
            if "dictionary" in kwargs:
                for table_name, df_obj in kwargs["dictionary"].items():
                    con.create_table(table_name, ibis.memtable(df_obj))
            self._data_sources[name] = con
            return

        if not hasattr(ibis, backend):
            raise ValueError(f"Backend '{backend}' is not supported by the underlying engine.")

        con_module = getattr(ibis, backend)
        con = con_module.connect(**kwargs)
            
        self._data_sources[name] = con

    def get_table(self, data_source_name: str, table_name: str) -> Any:
        """Retrieve a table from a registered data source."""
        if data_source_name not in self._data_sources:
            raise ValueError(f"Data source '{data_source_name}' not found.")
        return self._data_sources[data_source_name].table(table_name)

    def validate(self, table: Any, suite: ExpectationSuite) -> ExpectationSuiteValidationResult:
        """Validate a table against an ExpectationSuite."""
        validator = Validator(table=table)
        return validator.validate(suite)
