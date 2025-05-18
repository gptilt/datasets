from . import transform
from common import print
import polars as pl
import storage


def main(
    region: str,
    root: str,
    count: int = 1000,
    flush: bool = True,
    overwrite: bool = False,
):
    storage_basic = storage.StorageParquet(
        root=root,
        schema="basic",
        dataset="matches",
        tables=["matches", "participants", "events"],
    )

    # Load dataframes
    print("Loading dataframes from storage...")
    df_events = storage_basic.load_to_polars("events")

    df_participants = storage_basic.load_to_polars("participants").select([
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
    print("Enriching events with pregame data...")
    df_events_with_pregame_data = transform.enrich_events_with_pregame_data(
        df_events, df_participants
    )

    # Add inventory context to each event
    print("Enriching events with participant inventories...")
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
    print("Enriching events with participant levels...")
    df_events_with_levels = transform.enrich_events_with_levels(
        df_events_with_inventory,
        df_level_up_events=df_events.filter(pl.col("type") == "LEVEL_UP")
    )

    storage_gold = storage.StorageParquet(
        root=root,
        schema="ultimate",
        dataset="events",
        tables=["events"],
    )
    print("Writing enriched dataset to storage...")
    storage_gold.store_polars("events", df_events_with_levels)
    