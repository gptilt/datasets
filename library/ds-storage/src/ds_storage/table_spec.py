from dataclasses import dataclass

from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder, UNSORTED_SORT_ORDER


@dataclass(frozen=True)
class IcebergTableSpec:
    """
    A complete Iceberg table definition.

    Bundles the three things `Catalog.create_table` needs:
    - Schema;
    - Partition spec;
    - Sort order.

    `partition_spec`/`sort_order` default to Iceberg's unpartitioned/unsorted sentinels
    (an explicit `None` would override `create_table`'s real defaults
    and make it iterate `None.fields`),
    so an unpartitioned table only sets `schema`.
    """
    schema: Schema
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC
    sort_order: SortOrder = UNSORTED_SORT_ORDER
