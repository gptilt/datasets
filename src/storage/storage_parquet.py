import pyarrow as pa
import pyarrow.dataset as ds
from storage import Storage
from tqdm import tqdm


class StorageParquet(Storage):
    def __init__(self, root: str, dataset: str, schema: str, tables: list[str]):
        super().__init__(root, dataset, schema, tables, file_extension='parquet')
        self.target_batch_size = 1_000_000_000  # 1 GB
        self.buffer = {
            table_name: []
            for table_name in tables
        }
        

    def has_records_in_all_tables(self, **kwargs):
        """
        Check if the specified set of column_name=value pairs
        exist in all tables.
        """
        return all(
            self.has_record(table_name, column_name, value)
            for table_name in self.tables
            for column_name, value in kwargs.items()
        )


    def has_record(self, table_name: str, column_name: str, value):
        """
        Check if a column in a table has a specific value.
        """
        try:
            dataset = ds.dataset(self.table_path(table_name), format="parquet")
        except FileNotFoundError:
            # Dataset/directory doesn't exist
            return False
        try:
            scanner = dataset.scanner(columns=[column_name])
        except pa.lib.ArrowInvalid:
            # Column doesn't exist
            return False
        for batch in scanner.to_batches():
            if value in batch.column(column_name).to_pylist():
                return True
        return False
    

    def paTable_from_list_of_records(
        self,
        list_of_records: list[dict],
        schema: pa.Schema | None = None,
    ) -> pa.Table:
        try:
            # If `schema` is provided, it's used to construct the Arrow Table.
            if schema:
                table = pa.Table.from_pylist(list_of_records, schema=schema)
            else:
                table = pa.Table.from_pylist(list_of_records)
        except pa.lib.ArrowInvalid:
            # Find offending record
            for record in list_of_records:
                try:
                    table = pa.Table.from_pylist([record], schema=schema)
                    continue
                except pa.lib.ArrowInvalid as e:
                    raise ValueError(f"Failed to convert record {record} to Arrow Table: {e}")
        
        return table


    def store_batch(
        self,
        table_name: str,
        list_of_records: list[dict],
        schema: pa.Schema | None = None,
        **partition_columns: dict[str, str] | None,
    ) -> int:
        """
        Write a batch of data to the dataset.
        If the table is of insufficient size,
        save it to the buffer instead.
        """
        table = self.paTable_from_list_of_records(list_of_records, schema)
        
        if table.nbytes >= self.target_batch_size:
            nbytes = self.write_to_dataset(table_name, table, **partition_columns)
            return nbytes
        else:
            self.buffer[table_name].extend(list_of_records)
            return 0


    def flush(self, **partition_columns: dict[str, str] | None) -> int:
        """
        Write any remaining buffered records to disk.
        Should be called explicitly after processing is complete.
        """
        nbytes = 0
        
        for table_name, buffered_records in self.buffer.items():
            if buffered_records:
                table = pa.Table.from_pylist(buffered_records)
                nbytes += self.write_to_dataset(table_name, table, **partition_columns)
                self.buffer[table_name] = []
        
        return nbytes

    def write_to_dataset(
        self,
        table_name: str,
        table: pa.Table,
        **partition_columns: str | None
    ) -> int:
        """
        Add partition columns to the table and write it to the dataset.
        """
        for col_name, col_value in partition_columns.items():
            if col_value is not None:
                col_array = pa.array([col_value] * len(table))
                table = table.append_column(col_name, col_array)
        
        ds.write_dataset(
            data=table,
            base_dir=self.table_path(table_name),
            # basename_template=f"part-{ulid.new()}{'{i}'}parquet",
            existing_data_behavior="overwrite_or_ignore",
            format="parquet",
            partitioning=list(partition_columns.keys()) if partition_columns else None,
            partitioning_flavor="hive",
        )
        
        return table.nbytes
