import json
from pathlib import Path


class Storage:
    def __init__(self, root: str, dataset: str, schema: str, tables: list[str], file_extension: str = 'json'):
        self.schema = schema
        self.root_path = Path(root, dataset, schema)
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
    
    def partition(self, table_name: str, **partition_columns: dict[str, str] | None) -> Path:
        """
        Get the path for a partitioned table using k=v pairs.
        """
        partition = Path(
            self.table_path(table_name),
            *[f"{k}={v}" for k, v in partition_columns.items()],
        )
        Path(partition).mkdir(parents=True, exist_ok=True)
        return partition

    def find_files(self, partition: str, record: str) -> list[Path]:
        files = list(partition.rglob(f"{record}.{self.file_extension}"))
        if not files:
            raise FileNotFoundError(f"No file {record}.{self.file_extension} found in partition.")
        return files

    def read_file(self, table_name: str, record: str, **partition_columns: dict[str, str] | None) -> dict:
        """
        Read from raw storage.
        """
        path = self.find_files(self.partition(table_name, **partition_columns), record)[0]

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
        **partition_columns: dict[str, str] | None
    ):
        partition = self.partition(table_name, **partition_columns)
        with open(Path(partition, f"{record}.{self.file_extension}"), 'w') as f:
            if self.file_extension == 'json':
                json.dump(contents, f)
            else:
                raise NotImplementedError(f"Unsupported file extension: {self.file_extension}")
