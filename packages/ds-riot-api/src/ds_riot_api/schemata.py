from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

SCHEMATA = {
    'fact_player_rank': {
        'schema': Schema(
            NestedField(1, 'puuid', StringType(), required=True),
            NestedField(2, 'timestamp', TimestampType(), required=True),
            NestedField(3, "server", StringType(), required=True),
            NestedField(4, "league_id", StringType(), required=True),
            NestedField(5, "tier", StringType(), required=True),
            NestedField(6, "division", StringType(), required=True),
            NestedField(7, "rank", StringType(), required=True),
            NestedField(8, "league_points", IntegerType(), required=True),
            NestedField(9, "games_played", IntegerType(), required=True),
            NestedField(10, "wins", IntegerType(), required=True),
            NestedField(11, "losses", IntegerType(), required=True),
            NestedField(12, "win_rate", FloatType(), required=True),
            NestedField(13, "fresh_blood", BooleanType(), required=True),
            NestedField(14, "hot_streak", BooleanType(), required=True),
            NestedField(15, "inactive", BooleanType(), required=True),
            NestedField(16, "veteran", BooleanType(), required=True),
            
            # Primary Keys
            identifier_field_ids=[1, 2]
        ),
        'partition_spec': PartitionSpec(
            PartitionField(
                source_id=3, # Points to 'server'
                field_id=1000,
                transform=IdentityTransform(),
                name="server"
            ),
            PartitionField(
                source_id=2, # Points to 'timestamp'
                field_id=1001,
                transform=DayTransform(), # Iceberg's Hidden Partitioning
                name="timestamp_day"
            )
        ),
        'sort_order': SortOrder(
            SortField(
                source_id=3, # Sort by Server first...
                transform=IdentityTransform(),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_LAST
            ),
            SortField(
                source_id=1, # ...then by PUUID...
                transform=IdentityTransform(),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_LAST
            ),
            SortField(
                source_id=2, # ...then by Timestamp (most recent first)
                transform=IdentityTransform(),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_LAST
            )
        )
    }
}
