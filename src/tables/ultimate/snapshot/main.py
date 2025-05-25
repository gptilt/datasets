from . import transform
from common import print
import polars as pl
import storage


def main(
    list_of_match_ids: list[str],
    region: str,
    storage_basic: storage.StoragePartition,
    storage_ultimate: storage.StoragePartition,
):
    storage_from = storage.StoragePartition(
        root=storage_ultimate.root,
        schema="ultimate",
        dataset="events",
        tables=["events"],
        partition_col="region",
        partition_val=region
    )
    # Load dataframes
    print(f"[{region}] Loading dataframes from storage...")
    df_events = storage_from.load_to_polars(
        "events",
        matchId=list_of_match_ids
    )

    df_before_snapshot = (
        df_events
        .sort(["matchId", "timestamp"])
        .with_columns([
            pl.col("winningTeam").fill_null(strategy="backward").over("matchId")
        ])
        .filter(pl.col("timestamp").le(910000))
    )
    df_snapshot = transform.snapshot_from_events(df_before_snapshot)

    print(f"[{region}] Storing batch...")
    storage_ultimate.store_batch("snapshot", df_snapshot)

    return df_snapshot
