from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder, SortField
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
            NestedField(2, 'date', DateType(), required=True),
            NestedField(3, 'timestamp', TimestampType(), required=True),
            NestedField(4, "server", StringType(), required=True),
            NestedField(5, "league_id", StringType(), required=True),
            NestedField(6, "tier", StringType(), required=True),
            NestedField(7, "division", StringType(), required=True),
            NestedField(8, "rank", StringType(), required=True),
            NestedField(9, "league_points", IntegerType(), required=True),
            NestedField(10, "games_played", IntegerType(), required=True),
            NestedField(11, "wins", IntegerType(), required=True),
            NestedField(12, "losses", IntegerType(), required=True),
            NestedField(13, "win_rate", FloatType(), required=True),
            NestedField(14, "fresh_blood", BooleanType(), required=True),
            NestedField(15, "hot_streak", BooleanType(), required=True),
            NestedField(16, "inactive", BooleanType(), required=True),
            NestedField(17, "veteran", BooleanType(), required=True),
            # Primary Keys
            identifier_field_ids=[1, 2]
        ),
        'partition_spec': PartitionSpec(
            PartitionField(
                # Sort by date
                source_id=2,
                field_id=1000,
                transform="identity",
                name="date_partition"
            )
        ),
        'sort_order': SortOrder(
            SortField(
                # Sort by server
                source_id=4,
                transform="identity"
            )
        )
    }
}