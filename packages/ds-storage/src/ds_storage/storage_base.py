import dagster as dg
from pathlib import Path
from pydantic import Field
from typing import Annotated


NonEmptyStr = Annotated[str, Field(min_length=1, strip_whitespace=True)]


class TableNotFoundError(FileNotFoundError):
    pass


class Storage(dg.ConfigurableResource):
    root: NonEmptyStr
    dataset: NonEmptyStr
    schema_name: NonEmptyStr
    tables: list[NonEmptyStr]

    def validate_table_name(self, table_name: str):
        if table_name not in self.tables:
            raise TableNotFoundError(f"Table {table_name} not found in schema {self.schema_name}.")

    def root_path(self) -> Path:
        return Path(self.root, self.dataset, self.schema_name)
    
    def table_path(self, table_name: str) -> Path:
        self.validate_table_name(table_name)
        return Path(self.root_path(), table_name)

    def partition_path(self, table_name: str, **partition_columns: dict[str, str] | None) -> Path:
        """
        Get the path for a partitioned table using k=v pairs.
        """
        partition = Path(
            self.table_path(table_name),
            *[f"{k}={v}" for k, v in partition_columns.items()],
        )
        return partition