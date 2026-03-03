import polars as pl
import polars.selectors as cs
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA as sk_PCA


def enrich_events_with_pregame_data(
    df_events: pl.DataFrame,
    df_participants: pl.DataFrame,
) -> pl.DataFrame:
    """
    Create a DataFrame with participant frames enriched with pre-game data.
    This function filters the events DataFrame for participant frames and
    joins it with the participants DataFrame to add pre-game data such as
    championId, etc.
    Args:
        df_events (pl.DataFrame): DataFrame containing events.
        df_participants (pl.DataFrame): DataFrame containing participant information.
    Returns:
        pl.DataFrame: DataFrame containing participant frames with pre-game data.
    """
    # To each participant frame, add the pre-game data
    participant_exclusive_columns = [
        "currentGold", "totalGold",
        # "level",  # Level will be enriched with the last level-up event
        "xp",
        "minionsKilled", "jungleMinionsKilled",
        "championStats",
    ]
    df_participant_frames_with_pregame_data = df_events.filter(
        pl.col("type") == "PARTICIPANT_FRAME"
    ).select([
        "matchId", "participantId",
        "timestamp",
        "positionX", "positionY",
        *participant_exclusive_columns,
    ]).join(
        df_participants,
        on=["matchId", "participantId"],
        how="left",
    )
    # To each event, add the preceding participant frame, pivoted by participantId
    df_participant_frames_pivoted = df_participant_frames_with_pregame_data.pivot(
        on=["participantId"],
        index=["matchId", "timestamp"],
    )
    return df_events.filter(
        pl.col("type") != "PARTICIPANT_FRAME"
    ).drop(
        participant_exclusive_columns
    ).sort("matchId", "timestamp").join_asof(
        df_participant_frames_pivoted,
        on="timestamp",
        by=["matchId"],
        strategy="backward",
        allow_parallel=True,
    )


def enrich_events_with_inventory_data(
    df_events: pl.DataFrame,
    df_item_events: pl.DataFrame,
) -> pl.DataFrame:
    """
    Create a DataFrame with events enriched with inventory data.
    This function filters the events DataFrame for item events and
    joins it with the participants DataFrame to add inventory data.
    Args:
        df_events (pl.DataFrame): DataFrame containing events.
    Returns:
        pl.DataFrame: DataFrame containing events with inventory data.
    """
    # To each item event, add the inventory
    # Add inventory context to each event
    df_inventories = df_item_events.select(
        "matchId",
        "participantId",
        "eventId",
        "inventoryIds",
        "inventoryCounts"
    )
    
    df_inventories_pivoted = (
        df_inventories.pivot(
            on=["participantId"],
            index=["matchId", "eventId"],
            aggregate_function="last"  # Multiple item events can occur at the same timestamp
        )
        # Sort to ensure forward fill works correctly
        .sort(["matchId", "eventId"])
        # Fill forward to propagate the last known level
        .fill_null(strategy="forward")
    )

    # Join-asof inventory info into all events
    return df_events.sort(
        ["matchId", "eventId"]
    ).join_asof(
        df_inventories_pivoted,
        on="eventId",
        by=["matchId"],
        strategy="backward",  # get the last inventory at or before the event
        allow_parallel=True,
    )


def enrich_events_with_levels(
    df_events: pl.DataFrame,
    df_level_up_events: pl.DataFrame,
) -> pl.DataFrame:
    """
    Create a DataFrame with events enriched with level data.
    This function joins level_up events with the participants DataFrame to add level data.
    Args:
        df_events (pl.DataFrame): DataFrame containing events.
        df_level_up_events (pl.DataFrame): DataFrame containing level-up events.
    Returns:
        pl.DataFrame: DataFrame containing events with level data.
    """
    # Update participant levels to reflect the last level-up event
    # Sort data by matchId, participantId, and eventId
    df_level_ups = (
        df_level_up_events
        .select(["matchId", "participantId", "eventId", "level"])
        .pivot(
            on=["participantId"],
            index=["matchId", "eventId"],
            aggregate_function="last"  # Multiple level-ups can occur at the same timestamp
        )
        # Sort to ensure forward fill works correctly
        .sort(["matchId", "eventId"])
        # Fill forward to propagate the last known level
        .fill_null(strategy="forward")
        .rename({str(i): f"level_{i}" for i in range(1, 11)})
    )
    # Join-asof level info into all events
    return df_events.sort(
        ["matchId", "eventId"]
    ).join_asof(
        df_level_ups,
        on="eventId",
        by=["matchId"],
        strategy="backward",  # get the last level-up at or before the event
        allow_parallel=True,
    ).with_columns(
        cs.starts_with("level_").fill_null(1)
    )

def enrich_events_with_event_type_embeddings(
    df_events: pl.DataFrame
):
    map_of_event_types_to_keywords = {
        "PARTICIPANT_FRAME": "CHAMPION STATUS",
        "ITEM_DESTROYED": "CHAMPION ITEM DESTROYED",
        "ITEM_PURCHASED": "CHAMPION ITEM PURCHASED",
        "ITEM_SOLD": "CHAMPION ITEM SOLD",
        "ITEM_UNDO": "CHAMPION ITEM UNDO",
        "LEVEL_UP": "CHAMPION LEVEL UP",
        "CHAMPION_TRANSFORM": "CHAMPION LEVEL TRANSFORM",
        "SKILL_LEVEL_UP": "CHAMPION LEVEL SKILL",
        "BUILDING_ASSIST": "TAKEDOWN BUILDING ASSIST",
        "BUILDING_KILL": "TAKEDOWN BUILDING KILL",
        "TURRET_PLATE_DESTROYED": "TAKEDOWN BUILDING PLATE KILL",
        "CHAMPION_ASSIST": "TAKEDOWN CHAMPION ASSIST",
        "CHAMPION_KILL": "TAKEDOWN CHAMPION KILL",
        "RESPAWN": "TAKEDOWN CHAMPION RESPAWN",
        "ELITE_MONSTER_ASSIST": "TAKEDOWN MONSTER ASSIST",
        "ELITE_MONSTER_KILL": "TAKEDOWN MONSTER KILL",
        "WARD_KILL": "TAKEDOWN WARD KILL",
        "WARD_PLACED": "TAKEDOWN WARD SPAWN",
        "DRAGON_SOUL_GIVEN": "SYSTEM MONSTER DRAGON SOUL",
        "FEAT_UPDATE": "SYSTEM TAKEDOWN FEAT",
        "OBJECTIVE_BOUNTY_PRESTART": "SYSTEM BOUNTY PRESTART",
        "OBJECTIVE_BOUNTY_START": "SYSTEM BOUNTY START",
        "OBJECTIVE_BOUNTY_FINISH": "SYSTEM BOUNTY END",
        "GAME_END": "SYSTEM GAME END",
        "PAUSE_END": "SYSTEM GAME START",
    }

    model = SentenceTransformer("all-MiniLM-L6-v2")  # 384-dim embeddings

    # Reduce to the number of components that explain at least 90% of the variance
    pca = sk_PCA(n_components=0.9)
    array_of_reduced_embeddings = pca.fit_transform(
        model.encode(list(map_of_event_types_to_keywords.values()))
    )

    map_of_event_types_to_reduced_embeddings = {
        # Map to reduced embeddings, and convert to list to prevent serialization issues with Polars
        k: v.tolist() for k, v in dict(zip(
            list(map_of_event_types_to_keywords.keys()),
            array_of_reduced_embeddings
        )).items()
    }

    return df_events.with_columns(
        pl.col("type")
        .replace_strict(
            map_of_event_types_to_reduced_embeddings,
            default=[]
        )
        .alias("type_embeddings")
    )
