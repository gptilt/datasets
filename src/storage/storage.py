import dagster as dg
from pathlib import Path
from pydantic import Field
from typing import Annotated


NonEmptyStr = Annotated[str, Field(min_length=1, strip_whitespace=True)]


class TableNotFoundError(FileNotFoundError):
    pass


class Storage(dg.ConfigurableResource):
    root: NonEmptyStr
    schema_name: NonEmptyStr
    dataset: NonEmptyStr
    tables: list[NonEmptyStr]


    def root_path(self) -> Path:
        return Path(self.root, self.schema_name, self.dataset)
    
    def table_path(self, table_name: str) -> Path:
        if table_name not in self.tables:
            raise TableNotFoundError(f"Table {table_name} not found in schema {self.schema_name}.")
        return Path(self.root_path(), table_name)

    def partition_path(self, table_name: str, **partition_columns: dict[str, str] | None) -> Path:
        """
        Get the path for a partitioned table using k=v pairs.
        """
        partition = Path(
            self.table_path(table_name),
            *[f"{k}={v}" for k, v in partition_columns.items()],
        )
        Path(partition).mkdir(parents=True, exist_ok=True)
        return partition
