from . import transform
from common import print, work_generator
import polars as pl
import storage


def enrich_events(
    list_of_match_ids: list[str],
    region: str,
    storage_basic: storage.StoragePartition,
    storage_ultimate: storage.StoragePartition
):
    # Load dataframes
    print(f"[{region}] Loading dataframes from storage...")
    df_events = storage_basic.load_to_polars(
        "events",
        matchId=list_of_match_ids
    )

    df_participants = storage_basic.load_to_polars(
        "participants",
        matchId=list_of_match_ids
    ).select([
        "matchId",
        "participantId",
        "championId",
        "teamId",
        "summoner1Id",
        "summoner2Id",
        "rune_primary_0",
        "rune_primary_1",
        "rune_primary_2",
        "rune_primary_3",
        "rune_secondary_0",
        "rune_secondary_1",
        "rune_shard_offense",
        "rune_shard_flex",
        "rune_shard_defense",
    ])

    # To each participant frame, add the pre-game data
    print(f"[{region}] Enriching events with pregame data...")
    df_events_with_pregame_data = transform.enrich_events_with_pregame_data(
        df_events, df_participants
    )

    # Add inventory context to each event
    print(f"[{region}] Enriching events with participant inventories...")
    df_events_with_inventory = transform.enrich_events_with_inventory_data(
        df_events_with_pregame_data,
        df_item_events=df_events.filter(
            pl.col("type").is_in([
                "ITEM_PURCHASED",
                "ITEM_SOLD",
                "ITEM_DESTROYED",
                "ITEM_UNDO",
            ])
        )
    )

    # Update participant levels to reflect the last level-up event
    print(f"[{region}] Enriching events with participant levels...")
    df_events_with_levels = transform.enrich_events_with_levels(
        df_events_with_inventory,
        df_level_up_events=df_events.filter(pl.col("type") == "LEVEL_UP")
    )

    print(f"[{region}] Storing batch...")
    storage_ultimate.store_batch("events", df_events_with_levels)

    return df_events_with_levels


def main(
    region: str,
    root: str,
    count: int = 1000,
    flush: bool = True,
    overwrite: bool = False,
):
    storage_basic = storage.StoragePartition(
        root=root,
        schema="basic",
        dataset="matches",
        tables=["matches", "participants", "events"],
        partition_col="region",
        partition_val=region
    )
    storage_ultimate = storage.StoragePartition(
        root=root,
        schema="ultimate",
        dataset="events",
        tables=["events"],
        partition_col="region",
        partition_val=region
    )

    list_of_match_ids = storage_basic.load_to_polars("matches", ["matchId"])
    
    for _ in work_generator(
        list_of_match_ids,
        enrich_events,
        descriptor=region,
        step=count // 10,
        # kwargs
        storage_basic=storage_basic,
        storage_ultimate=storage_ultimate
    ):
        continue

    if flush:
        storage_ultimate.flush()
    