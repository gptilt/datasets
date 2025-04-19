import json
import os
from pathlib import Path


DIR_DATASET = lambda dataset, schema, region: f"{os.environ["DIR_ROOT"]}/{dataset}/{schema}/{region}"


class Storage:
    def __init__(self, dataset: str, schema: str, region: str, tables: list[str], file_extension: str = 'json'):
        self.schema = schema
        self.region = region
        self.root_path = Path(DIR_DATASET(dataset, schema, region))
        self.tables = tables
        self.file_extension = file_extension

        for table in tables:
            Path.mkdir(self.table_path(table), parents=True, exist_ok=True)
    
    def total_size_in_gb(self) -> float:
        return sum(
            f.stat().st_size
            for f in self.root_path.rglob('*') if f.is_file()
        ) / (1014 ** 3)  # GB
    
    def table_path(self, table_name: str) -> Path:
        return Path(self.root_path, table_name)
    
    def partition(self, table_name: str, **partition_columns) -> Path:
        """
        Get the path for a partitioned table using k=v pairs.
        """
        partition = Path(
            self.table_path(table_name),
            *[f"{k}={v}" for k, v in partition_columns.items()],
        )
        Path(partition).mkdir(parents=True, exist_ok=True)
        return partition

    def find_file(self, table_name: str, record: str) -> Path:
        try:
            return list(self.table_path(table_name).rglob(f"{record}.{self.file_extension}"))[0]
        except IndexError:
            raise FileNotFoundError(f"File {record}.{self.file_extension} not found in table {table_name}.")
    
    def read_file(self, table_name: str, record: str) -> dict:
        """
        Read from raw storage.
        """
        path = self.find_file(table_name, record)

        with open(path, 'r') as f:
            if self.file_extension == 'json':
                return json.load(f)
            else:
                raise NotImplementedError(f"Unsupported file extension: {self.file_extension}")
    
    def store_file(
        self,
        table_name: str,
        record: str,
        contents: any,
        **partition_columns: str,
    ):
        partition = self.partition(table_name, **partition_columns)
        with open(Path(partition, f"{record}.{self.file_extension}"), 'w') as f:
            if self.file_extension == 'json':
                json.dump(contents, f)
            else:
                raise NotImplementedError(f"Unsupported file extension: {self.file_extension}")
