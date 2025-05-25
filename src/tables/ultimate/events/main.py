from . import transform
from common import print
import polars as pl
import storage


def main(
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
    df_events_with_pregame_data = transform.enrich_events_with_pregame_data(
        df_events, df_participants
    )

    # Add inventory context to each event
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
    df_events_with_levels = transform.enrich_events_with_levels(
        df_events_with_inventory,
        df_level_up_events=df_events.filter(pl.col("type") == "LEVEL_UP")
    )

    # Add general match information
    df_matches = storage_basic.load_to_polars(
        "matches",
        matchId=list_of_match_ids
    ).select([
        "gameStartTimestamp",
        "gameVersion",
        "matchId",
        "platformId"
    ])
    df_events_with_matches = df_events_with_levels.join(
        df_matches,
        on=["matchId"],
        how="inner"
    )

    print(f"[{region}] Storing batch...")
    storage_ultimate.store_batch("events", df_events_with_matches)

    return df_events_with_levels
