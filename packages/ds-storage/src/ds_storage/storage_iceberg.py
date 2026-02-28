import pyarrow as pa
from pydantic import PrivateAttr
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from .storage_base import Storage, NonEmptyStr


class StorageIceberg(Storage):
    warehouse_name: NonEmptyStr
    catalog_uri: NonEmptyStr
    partition_spec: dict[NonEmptyStr, PartitionSpec]
    schemata: dict[NonEmptyStr, Schema]
    token: NonEmptyStr
    
    # Declare a private attribute that Pydantic/Dagster ignores during serialization
    _catalog: object = PrivateAttr(default=None)


    def full_table_name(self, table_name: str):
        return f"{self.dataset}.{self.schema_name}.{table_name}"


    def create_table_if_not_exists(self, table_name: str):
        full_name = self.full_table_name(table_name)

        partition_spec = self.partition_spec.get(table_name)
        if partition_spec is None:
            raise ValueError(f"No partition spec found for table: {table_name}")
        schema = self.schemata.get(table_name)
        if schema is None:
            raise ValueError(f"No schema found for table: {table_name}")

        if not self.catalog.table_exists(full_name):
            self.catalog.create_table(full_name, schema=schema, partition_spec=partition_spec)


    def get_table(self, table_name: str):
        self.create_table_if_not_exists(table_name)

        return self.catalog.load_table(self.full_table_name(table_name))


    @property
    def catalog(self):

        if self._catalog is None:
            self._catalog = RestCatalog(
                name=self.root,
                warehouse_name=self.warehouse_name,
                catalog_uri=self.catalog_uri,
                token=self.token
            )

        self.catalog.create_namespace_if_not_exists()

        return self._catalog


    def upsert_records(
        self,
        table_name: str,
        records: list[dict],
    ):
        table = self.get_table(table_name)
        table.upsert(pa.Table.from_pylist(
            records,
            schema=table.schema
        ))