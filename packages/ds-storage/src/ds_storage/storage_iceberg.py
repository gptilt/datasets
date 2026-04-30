from ds_common import convert_polars_df_to_pyarrow_table_using_iceberg_schema
import duckdb
from functools import reduce
import polars as pl
import pyarrow as pa
from pydantic import PrivateAttr
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, EqualTo, BooleanExpression
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import DataScan, Table
from pyiceberg.table.sorting import SortOrder
import random
import time
from .storage_base import Storage, NonEmptyStr


DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 2


class StorageIceberg(Storage):
    """
    Iceberg storage resource backed by a REST catalog.

    Attributes
    ----------
    warehouse_name : str
        Iceberg warehouse identifier.
    catalog_uri : str
        REST catalog URI.
    token : str
        Authentication token for the catalog.
    """
    warehouse_name: NonEmptyStr
    catalog_uri: NonEmptyStr
    token: NonEmptyStr
    
    # Declare a private attribute that Pydantic/Dagster ignores during serialization
    _catalog: object = PrivateAttr(default=None)


    def namespace(self):
        return f"{self.dataset}.{self.schema_name}"


    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = RestCatalog(
                name=self.root,
                warehouse=self.warehouse_name,
                uri=self.catalog_uri,
                token=self.token
            )

        parts = self.namespace().split(".")
        for i in range(1, len(parts) + 1):
            self._catalog.create_namespace_if_not_exists(tuple(parts[:i]))

        return self._catalog


    def full_table_name(self, table_name: str):
        return f"{self.namespace()}.{table_name}"


    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: Schema = None,
        partition_spec: PartitionSpec = None,
        sort_order: SortOrder = None,
    ) -> Table:
        full_name = self.full_table_name(table_name)

        if not self.catalog.table_exists(full_name):
            return self.catalog.create_table(
                full_name,
                schema=schema,
                partition_spec=partition_spec,
                sort_order=sort_order
            )
        
        return self.catalog.load_table(full_name)


    def get_table_schema(self, table_name: str):
        return self.catalog.load_table(self.full_table_name(table_name)).schema()


    def scan_table(
        self,
        table_name: str,
        selected_fields: list[str] = None,
        row_filter: BooleanExpression = None,
        **partition_columns: dict[str, str] | None,
    ) -> DataScan:
        """
        Read an Iceberg table and return a DataScan object.

        Parameters
        ----------
        table_name : str
            Name of the table to read.
        selected_fields : list[str], optional
            List of column names to select. Reads all columns if not specified.
        row_filter : BooleanExpression, optional
            Filter expression to apply to the rows.
        **partition_columns
            Partition key-value pairs to filter by e.g. server='na1', date='2026-01-01'
        """
        table = self.catalog.load_table(self.full_table_name(table_name))

        scan_kwargs = {}
        filters = []

        if selected_fields:
            scan_kwargs['selected_fields'] = tuple(selected_fields)

        if row_filter is not None:
            filters.append(row_filter)

        if partition_columns:
            filters.extend([
                EqualTo(k, v)
                for k, v in partition_columns.items()
                if v is not None
            ])

        if filters:
            scan_kwargs['row_filter'] = reduce(And, filters)

        return table.scan(**scan_kwargs)


    def write_table(
        self,
        table_name: str,
        pyarrow_table: pa.Table,
        mode: str = 'append',
        retries: int = DEFAULT_RETRIES,
        backoff_factor: int = DEFAULT_BACKOFF_FACTOR,
    ):
        """
        Attempt to write a PyArrow table to Iceberg, retrying on failure.
        """
        table = self.catalog.load_table(self.full_table_name(table_name))

        attempt = 0
        while attempt < retries:
            try:
                table.refresh()  # force re-read of current metadata
                match mode:
                    case 'append':
                        table.append(pyarrow_table)
                    case 'upsert':
                        table.upsert(pyarrow_table)
                    case _:
                        raise ValueError(f"Unsupported write mode: '{mode}'")
                return  # Success
            except Exception as e:
                attempt += 1
                wait_time = backoff_factor ** attempt + random.random()  # jitter
                print(f"Write failed (attempt {attempt}/{retries}): {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
        # If we exit the loop, all retries failed
        raise RuntimeError(f"Failed to write to '{table_name}' with mode '{mode}' after {retries} attempts")


    def write_dataframe_to_table(
        self,
        table_name: str,
        df: pl.DataFrame,
        mode: str = 'append',
        retries: int = DEFAULT_RETRIES,
        backoff_factor: int = DEFAULT_BACKOFF_FACTOR,
    ):
        """
        Load a Polars DataFrame into an Iceberg table.
        """
        schema = self.get_table_schema(table_name)

        self.write_table(
            table_name,
            convert_polars_df_to_pyarrow_table_using_iceberg_schema(df, schema),
            mode=mode,
            retries=retries,
            backoff_factor=backoff_factor
        )


    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Returns a DuckDB connection with each table in `self.tables` registered as
        an in-memory Arrow scan, queryable by its bare table name.

        Tables that don't yet exist in the catalog are skipped silently — useful
        for first-time exploration before any writes have happened. Re-call
        `connect()` after writes to refresh.
        """
        con = duckdb.connect()
        for table_name in self.tables:
            full_name = self.full_table_name(table_name)
            try:
                arrow = self.catalog.load_table(full_name).scan().to_arrow()
            except Exception:
                continue
            con.register(table_name, arrow)
        return con

    def write_records_to_table(
        self,
        table_name: str,
        list_of_records: list[dict],
        mode: str = 'upsert',
        retries: int = DEFAULT_RETRIES,
        backoff_factor: int = DEFAULT_BACKOFF_FACTOR
    ):
        schema = self.get_table_schema(table_name)

        self.write_table(
            table_name,
            pa.Table.from_pylist(list_of_records, schema=schema.as_arrow()),
            mode=mode,
            retries=retries,
            backoff_factor=backoff_factor,
        )
