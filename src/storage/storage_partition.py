from common import print
from collections.abc import Iterable
import datasets
import os
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pa_ds
from storage import Storage


def to_polars(batch, schema: pl.Schema | None = None):
    if isinstance(batch, pl.DataFrame):
        return batch
    elif isinstance(batch, Iterable):
        return pl.from_dicts(batch, schema)
    else:
        raise ValueError(f"Unsupported batch type {type(batch)}.")


class StoragePartition(Storage):
    def __init__(
        self,
        root: str,
        schema: str,
        dataset: str,
        tables: list[str],
        partition_col: str = None,
        partition_val: str = None,
        table_schema: dict[str, pa.Schema] | None = {},
    ):
        super().__init__(root, schema, dataset, tables, file_extension='parquet')
        self.target_batch_size = 2_000_000_000  # 2 GB
        self.partition_col, self.partition_val = partition_col, partition_val
        
        self.table_schema = table_schema
        self.buffer = {}
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


    def _add_partition_column_to_df(self, df: pl.DataFrame) -> pl.DataFrame:
        if self.partition_col not in df.columns:
            df = df.with_columns([
                pl.lit(self.partition_val).alias(self.partition_col)
            ])
        return df


    def _flush_table(
        self,
        table_name: str,
        df: pl.DataFrame,
    ) -> int:
        """
        Write to disk using pyarrow datasets.
        Overwrites existing dataset for simplicity.
        """
        df = self._add_partition_column_to_df(df)

        ds = datasets.Dataset(
            df.to_arrow(),
            split=self.split(),
            info=datasets.DatasetInfo()
        )
        ds.to_parquet(f"{self.table_path(table_name)}/{self.split()}-{self.shard_id[table_name]:05d}.parquet")
        self.shard_id[table_name] += 1

        return df.estimated_size()
    

    def flush(self) -> int:
        """
        Write any remaining buffered records to disk.
        Should be called explicitly after processing is complete.
        """
        nbytes = 0
        
        for table_name in list(self.buffer.keys()):
            nbytes += self._flush_table(table_name, self.buffer.pop(table_name))
        
        return nbytes
    

    def store_batch(
        self,
        table_name: str,
        batch: list[dict] | pl.DataFrame,
    ) -> int:
        """
        Writes a batch of data to the dataset.

        Records are appended to the existing buffered list of records.
        If the complete table is of sufficient size, the buffer is flushed.

        Args:
            table_name (str): Name of the table to save the batch to.
            batch (list[dict] | pl.DataFrame): Batch to store.
        """
        df = to_polars(batch, self.table_schema.get(table_name))
        df = df.select(sorted(df.columns))

        if table_name in self.buffer:
            self.buffer[table_name] = df = pl.concat([df, self.buffer.get(table_name)])
        else:
            self.buffer[table_name] = df

        if df.estimated_size() >= self.target_batch_size:
            nbytes = self._flush_table(table_name, self.buffer.pop(table_name, df))
            return nbytes
        else:
            return 0


    def load_to_polars(
        self,
        table_name: str,
        columns: list[str] | None = "*",
        **filters: dict[str, str] | None
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
                schema=self.table_schema.get(table_name)
            )
        except FileNotFoundError:
            print(f"No parquet files found for table '{table_name}' at {table_path}.")
            return pl.DataFrame()

        # Apply column selection
        lazy_df = lazy_df.select(columns)

        for filter_col, filter_val in filters.items():
            if isinstance(filter_val, Iterable) and not isinstance(filter_val, (str, bytes)):
                lazy_df = lazy_df.filter(pl.col(filter_col).is_in(filter_val))
            else:
                lazy_df = lazy_df.filter(pl.col(filter_col) == filter_val)

        return lazy_df.collect()
