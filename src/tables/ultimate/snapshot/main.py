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
        partition_val=region,
        split=storage_ultimate.split
    )
    # Load dataframes
    print(f"[{region}] Loading dataframes from storage...")
    df_events = storage_from.load_to_polars(
        "events",
        matchId=list_of_match_ids
    )

    # Find matchIds with any timestamp ≥ snapshot time
    SNAPSHOT_TIME_IN_MS = 900_000
    df_long_enough_matches = df_events.group_by("matchId").agg(
        pl.col("timestamp").max().alias("max_timestamp")
    ).filter(
        pl.col("max_timestamp") >= SNAPSHOT_TIME_IN_MS
    )["matchId"]
    
    df_before_snapshot = (
        df_events
        # Ensure all matches are long enough
        .filter(
            pl.col("matchId").is_in(df_long_enough_matches)
        )
        # Sort by timestamp and backfill the winningTeam (target variable)
        .sort(["matchId", "timestamp"])
        .with_columns([
            pl.col("winningTeam").fill_null(strategy="backward").over("matchId")
        ])
        # Add a 10 second tolerance, to ensure the snapshot's participant frame is included.
        .filter(pl.col("timestamp").le(SNAPSHOT_TIME_IN_MS + 10_000))
    )
    df_snapshot = transform.snapshot_from_events(df_before_snapshot)

    print(f"[{region}] Storing batch...")
    storage_ultimate.store_batch("snapshot", df_snapshot)

    return df_snapshot
