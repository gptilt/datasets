import pyarrow as pa
from pydantic import PrivateAttr
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder
import random
import time
from .storage_base import Storage, NonEmptyStr


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
        sort_order: SortOrder = None
    ):
        full_name = self.full_table_name(table_name)

        if not self.catalog.table_exists(full_name):
            return self.catalog.create_table(
                full_name,
                schema=schema,
                partition_spec=partition_spec,
                sort_order=sort_order
            )
        
        return self.catalog.load_table(full_name)


    def upsert_table(
        self,
        table_name: str,
        pyarrow_table: pa.Table,
        schema: Schema = None,
        partition_spec: PartitionSpec = None,
        sort_order: SortOrder = None,
        retries: int = 3,
        backoff_factor: int = 2,
    ):
        """
        Attempt to upload a DataFrame to S3, retrying on failure.
        """
        table = self.create_table_if_not_exists(table_name, schema, partition_spec, sort_order)

        attempt = 0
        while attempt < retries:
            try:
                table.upsert(pyarrow_table)
                return  # Success
            except Exception as e:
                attempt += 1
                wait_time = backoff_factor ** attempt + random.random()  # jitter
                print(f"Upsert failed (attempt {attempt}/{retries}): {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
        # If we exit the loop, all retries failed
        raise RuntimeError(f"Failed to upsert {table_name} after {retries} attempts")


    def upsert_records(
        self,
        table_name: str,
        list_of_records: list[dict],
        schema: Schema = None,
        partition_spec: PartitionSpec = None,
        sort_order: SortOrder = None
    ):
        self.upsert_table(
            table_name,
            pa.Table.from_pylist(list_of_records, schema=schema.as_arrow()),
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order
        )
    