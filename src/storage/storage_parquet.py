from common import print
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from storage import Storage


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


    def flush(
        self,
        dict_of_table_schema: dict[str, pa.Schema] = {},
        **partition_columns: dict[str, str] | None
    ) -> int:
        """
        Write any remaining buffered records to disk.
        Should be called explicitly after processing is complete.
        """
        nbytes = 0
        
        for table_name, buffered_records in self.buffer.items():
            if buffered_records:
                table = self.paTable_from_list_of_records(buffered_records, dict_of_table_schema.get(table_name))
                nbytes += self.write_to_dataset(table_name, table, **partition_columns)
                self.buffer[table_name] = []
        
        return nbytes

    def write_to_dataset(
        self,
        table_name: str,
        table: pa.Table,
        **partition_columns: dict[str, str] | None
    ) -> int:
        """
        Add partition columns to the table and write it to the dataset.
        """
        for col_name, col_value in partition_columns.items():
            if col_value is not None:
                col_array = pa.array([col_value] * len(table))
                table = table.append_column(col_name, col_array)
        
        print(f"Dataset schema: {table.schema}")

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

    def load_to_polars(
        self,
        table_name: str,
        columns: list[str] | None = None,
        **partition_filters: str | None
    ) -> pl.DataFrame:
        """
        Loads a partitioned Parquet dataset into a Polars DataFrame,
        optionally filtering by partition values and selecting columns.

        Args:
            table_name (str): The name of the table to load.
            columns (list[str], optional): List of columns to select.
                If None, selects all columns. Defaults to None.
            **partition_filters (str, optional): Keyword arguments representing
                partition columns and their desired values for filtering.
        Returns:
            pl.DataFrame: The Polars DataFrame containing the loaded and filtered data.
                Returns an empty DataFrame if the dataset or specified
                partitions do not exist or if an error occurs.
        """
        table_base_path = self.table_path(table_name)
        print(f"Attempting to load table '{table_name}' from: {table_base_path}")

        try:
            lazy_df = pl.scan_parquet(
                f"{str(table_base_path)}/**/*.parquet",
                hive_partitioning=True,
            )
        except FileNotFoundError:
            print(f"No parquet files found for table '{table_name}' at {table_base_path}.")
            return pl.DataFrame()

        # Apply partition filters
        if partition_filters:
            print(f"Applying partition filters: {partition_filters}")
            for col, value in partition_filters.items():
                if value is None:
                    print(f"Warning: Skipping filter for column '{col}' with None value.")
                    continue
                try:
                    lazy_df = lazy_df.filter(pl.col(col) == str(value))
                except pl.exceptions.ColumnNotFoundError:
                    print(f"Warning: Partition column '{col}' not found in schema. Skipping filter.")

        # Apply column selection
        if columns:
            print(f"Selecting columns: {columns}")
            select_cols = columns.copy()
            schema_names = lazy_df.collect_schema().names()

            for p_col in (partition_filters or {}):
                if p_col not in select_cols and p_col in schema_names:
                    select_cols.append(p_col)

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


    def store_polars(
        self,
        table_name: str,
        df: pl.DataFrame,
        **partition_columns: str | None,
    ) -> int:
        """
        Write a Polars DataFrame to the dataset as a Parquet file,
        converting it to an Arrow Table and applying partitioning if needed.

        Args:
            table_name (str): The name of the table to write to.
            df (pl.DataFrame): The Polars DataFrame to write.
            **partition_columns (str, optional): Columns to partition by.

        Returns:
            int: Number of bytes written.
        """
        if df.is_empty():
            print(f"Skipping write for empty DataFrame to table '{table_name}'")
            return 0

        table = df.to_arrow()
        return self.write_to_dataset(table_name, table, **partition_columns)
