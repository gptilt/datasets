from common import print
import datasets
import os
from pathlib import Path
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pa_ds
from storage import Storage


class StoragePartition(Storage):
    def __init__(
        self,
        root: str,
        schema: str,
        dataset: str,
        tables: list[str],
        partition_col: str = None,
        partition_val: str = None
    ):
        super().__init__(root, schema, dataset, tables, file_extension='parquet')
        self.target_batch_size = 1_000_000_000  # 1 GB
        self.partition_col, self.partition_val = partition_col, partition_val
        
        self.buffer = {
            table_name: []
            for table_name in tables
        }
        self.shard_id = { table_name: 0 for table_name in tables}


    def split(self) -> str:
        return "_".join((self.partition_col, self.partition_val))
    

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
            dataset = pa_ds.dataset(self.table_path(table_name), format="parquet")
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
    

    def list_of_partitions(
        self, table_name: str
    ) -> int:    
        table_path = self.table_path(table_name)
        leaf_dirs = []

        for dirpath, dirnames, filenames in os.walk(table_path):
            if not dirnames:  # no subdirectories => leaf directory (partition)
                rel_path = os.path.relpath(dirpath, table_path)
                if rel_path == '.':
                    leaf_dirs.append(())  # root is the only leaf
                else:
                    leaf_dirs.append(tuple(rel_path.split(os.sep)))

        return leaf_dirs


    def _add_partition_column_to_arrow_table(
        self,
        table: pa.Table,
    ) -> pa.Table:
        if self.partition_col not in table.schema.names:
            col_array = pa.array([self.partition_val] * len(table))
            table = table.append_column(self.partition_col, col_array)
        
        return table


    def _arrow_table_from_list_of_records(
        self,
        list_of_records: list[dict],
        schema: pa.Schema | None = None,
    ) -> pa.Table:
        try:
            # If `schema` is provided, it's used to construct the Arrow Table.
            if schema:
                return pa.Table.from_pylist(list_of_records, schema=schema)
            else:
                return pa.Table.from_pylist(list_of_records)
        except pa.lib.ArrowInvalid:
            # Troubleshoot single offending records
            for record in list_of_records:
                try:
                    pa.Table.from_pylist([record], schema=schema)
                except pa.lib.ArrowInvalid as e:
                    raise ValueError(f"Failed to convert record {record} to Arrow Table: {e}")
            raise

    
    def _flush_table(
        self,
        table_name: str,
        table: pa.Table,
    ) -> int:
        """
        Write to disk using pyarrow datasets.
        Overwrites existing dataset for simplicity.
        """
        table = self._add_partition_column_to_arrow_table(table)
        nbytes = table.nbytes

        ds = datasets.Dataset(
            table,
            split=self.split(),
            info=datasets.DatasetInfo()
        )
        ds.to_parquet(f"{self.table_path(table_name)}/{self.split()}-{self.shard_id[table_name]:05d}.parquet")
        self.shard_id[table_name] += 1

        return nbytes
    

    def flush(
        self,
        dict_of_table_schema: dict[str, pa.Schema] = {},
    ) -> int:
        """
        Write any remaining buffered records to disk.
        Should be called explicitly after processing is complete.
        """
        nbytes = 0
        
        for table_name, buffered_records in self.buffer.items():
            if not buffered_records:
                continue
            schema = dict_of_table_schema.get(table_name)
            table = self._arrow_table_from_list_of_records(buffered_records, schema)
            nbytes += self._flush_table(table_name, table)
            self.buffer[table_name] = []
        
        return nbytes
    

    def store_batch(
        self,
        table_name: str,
        list_of_records: list[dict],
        schema: pa.Schema | None = None,
    ) -> int:
        """
        Writes a batch of data to the dataset.

        Records are appended to the existing buffered list of records.
        If the complete table is of sufficient size, the buffer is flushed.
        """
        self.buffer[table_name].extend(list_of_records)
        table = self._arrow_table_from_list_of_records(self.buffer[table_name], schema)

        if table.nbytes >= self.target_batch_size:
            nbytes = self._flush_table(table_name, table)
            self.buffer[table_name] = []  # Clear buffer after writing
            return nbytes
        else:
            return 0


    def store_polars(
        self,
        table_name: str,
        df: pl.DataFrame,
    ) -> int:
        """
        Write a Polars DataFrame to the dataset as a Parquet file,
        converting it to an Arrow Table and applying partitioning if needed.

        Args:
            table_name (str): The name of the table to write to.
            df (pl.DataFrame): The Polars DataFrame to write.

        Returns:
            int: Number of bytes written.
        """
        if df.is_empty():
            print(f"Skipping write for empty DataFrame to table '{table_name}'")
            return 0

        table = df.to_arrow()
        return self._flush_table(table_name, table)


    def load_to_polars(
        self,
        table_name: str,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Loads a partitioned Parquet dataset into a Polars DataFrame,
        optionally filtering by partition values and selecting columns.

        Args:
            table_name (str): The name of the table to load.
            columns (list[str], optional): List of columns to select.
                If None, selects all columns. Defaults to None.
        Returns:
            pl.DataFrame: The Polars DataFrame containing the loaded and filtered data.
                Returns an empty DataFrame if the dataset or specified
                partitions do not exist or if an error occurs.
        """
        table_path = self.table_path(table_name)
        print(f"Attempting to load table '{table_name}' from: {table_path}")

        try:
            lazy_df = pl.scan_parquet(
                f"{str(table_path)}/{self.split()}-*.parquet",
                hive_partitioning=True,
            )
        except FileNotFoundError:
            print(f"No parquet files found for table '{table_name}' at {table_path}.")
            return pl.DataFrame()

        # Apply column selection
        if columns:
            print(f"Selecting columns: {columns}")
            select_cols = columns.copy()

            try:
                lazy_df = lazy_df.select(select_cols)
            except pl.exceptions.ColumnNotFoundError as e:
                print(f"Error selecting columns: {e}. Returning empty DataFrame.")
                return pl.DataFrame()

        # Collect the result
        print(f"Collecting data for '{table_name}'...")
        try:
            df = lazy_df.collect()
            print(f"Loaded DataFrame with shape: {df.shape}")
            return df
        except Exception as e:
            print(f"Failed to collect dataset for table '{table_name}': {e}")
            return pl.DataFrame()
