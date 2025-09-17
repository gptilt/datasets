import json
from pathlib import Path
from .storage import Storage


class StorageLocal(Storage):
    def __init__(self, root, schema, dataset, tables, file_extension = 'json'):
        super().__init__(root, schema, dataset, tables, file_extension)

        for table in tables:
            Path.mkdir(self.table_path(table), parents=True, exist_ok=True)

    def write(self, filename: str, contents: any):
        with open(filename, 'w') as f:
            json.dump(contents, f)