import json
from pathlib import Path
from .storage_base import Storage


class StorageLocal(Storage):
    file_extension: str = 'json'

    def init_tables(self):
        for table in self.tables:
            Path.mkdir(self.table_path(table), parents=True, exist_ok=True)

    def total_size_in_gb(self) -> float:
        return sum(
            f.stat().st_size
            for f in self.root_path().rglob('*') if f.is_file()
        ) / (1024 ** 3)  # GB

    def find_files(
        self,
        table_name: str,
        record: str,
        count: int = 1,
        **partition_columns: dict[str, str] | None
    ) -> list[Path]:
        partition = self.partition_path(table_name, **partition_columns)
        files = list(partition.rglob(f"{record}.{self.file_extension}"))
        if not files:
            raise FileNotFoundError(f"No file {record}.{self.file_extension} found in partition.")
        return files[:count if count > 0 else None]  # -1 returns all files
    
    def find_files_in_tables(
        self,
        record: str,
        list_of_tables: list[str],
        **partition_columns: dict[str, str] | None
    ) -> list[Path]:
        """
        Find files in all tables with the same record name.
        """
        files = []
        for table in list_of_tables:
            try:
                files.extend(self.find_files(table, record, count=-1, **partition_columns))
            except FileNotFoundError:
                pass
        if not files:
            raise FileNotFoundError(f"No file {record}.{self.file_extension} found in any table.")
        return files

    def load_file(self, path: str) -> dict:
        with open(path, 'r') as f:
            if self.file_extension == 'json':
                return json.load(f)
            else:
                raise NotImplementedError(f"Unsupported file extension: {self.file_extension}")
    
    def read_files(
        self,
        table_name: str,
        record: str,
        count: int = 1,
        **partition_columns: dict[str, str] | None
    ) -> list[dict]:
        """
        Read from raw storage.
        """
        paths = self.find_files(table_name, record, count, **partition_columns)

        return [
            self.load_file(path)
            for path in paths
        ][0]

    def store_file(
        self,
        table_name: str,
        record: str,
        contents: any,
        **partition_columns: dict[str, str] | None
    ):
        partition = self.partition_path(table_name, **partition_columns)
        filename = f"{partition}/{record}.{self.file_extension}"
        
        if self.file_extension == 'json':
            self.write(filename, contents)
        else:
            raise NotImplementedError(f"Unsupported file extension: {self.file_extension}")

    def write(self, filename: str, contents: any):
        with open(filename, 'w') as f:
            json.dump(contents, f)