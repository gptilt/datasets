from functools import reduce
import polars as pl


def counts_from_events(
    df: pl.DataFrame,
    filter: pl.Expr,
    prefix: str
):
    participant_ids = [str(i) for i in range(1, 11)]

    df_counts = (
        df
        .filter(filter)
        .group_by(["matchId", "participantId"])
        .len()
        .pivot(
            values="len",
            index="matchId",
            columns="participantId"
        )
    )

    # Ensure all participant columns exist
    for pid in participant_ids:
        if pid not in df_counts.columns:
            df_counts = df_counts.with_columns(pl.lit(0).alias(pid))

        # Rename participant columns with prefix
        df_counts = df_counts.rename({pid: f"{prefix}_{pid}"})

    # Reorder columns: matchId first, then sorted participants
    ordered_columns = ["matchId"] + [f"{prefix}_{pid}" for pid in participant_ids]
    
    return df_counts.select(ordered_columns)


def snapshot_from_events(
    df_events_up_to_snapshot: pl.DataFrame,
):
    df_participant_events_before_snapshot = df_events_up_to_snapshot.select([
        "matchId",
        "participantId",
        "type",
        "monsterType",
        "monsterSubType",
        "buildingType",
        "towerType",
        "victimId"
    ])

    df_kill_counts = counts_from_events(
        df_participant_events_before_snapshot,
        (pl.col("type") == "CHAMPION_KILL"),
        "kills"
    )
    df_assist_counts = counts_from_events(
        df_participant_events_before_snapshot,
        (pl.col("type") == "CHAMPION_ASSIST"),
        "assists"
    )

    df_death_counts = counts_from_events(
        df_participant_events_before_snapshot.drop("participantId").rename({"victimId": "participantId"}),
        (pl.col("type") == "CHAMPION_KILL"),
        "deaths"
    )

    list_of_df_building_kills = [
        counts_from_events(
            df_participant_events_before_snapshot,
            (pl.col("type") == "BUILDING_KILL")
            & (pl.col("buildingType") == building_type)
            & (pl.col("towerType") == tower_type),
            prefix=tower_type
        )
        for building_type in ["TOWER_BUILDING"]
        for tower_type in ["OUTER_TURRET", "INNER_TURRET", "INHIBITOR_TURRET", "NEXUS_TURRET"]
    ]

    list_of_df_monster_kills = [
        counts_from_events(
            df_participant_events_before_snapshot,
            (pl.col("type") == "ELITE_MONSTER_KILL")
                & (pl.col("monsterType") == monster_type),
            prefix=monster_type
        )
        for monster_type in ["HORDE", "RIFTHERALD"]
    ]

    list_of_dragon_types = [
        "AIR_DRAGON",
        "CHEMTECH_DRAGON",
        "EARTH_DRAGON",
        "FIRE_DRAGON",
        "HEXTECH_DRAGON",
        "WATER_DRAGON"
    ]
    list_of_df_dragon_kills = [
        counts_from_events(
            df_participant_events_before_snapshot,
            (pl.col("type") == "ELITE_MONSTER_KILL")
            & (pl.col("monsterSubType") == monster_sub_type),
            prefix=monster_sub_type
        )
        for monster_sub_type in list_of_dragon_types
    ]

    df_stats_at_snapshot = reduce(
        lambda left, right: left.join(right, on=["matchId"], how="left"),
        [
            df_kill_counts,
            df_death_counts,
            df_assist_counts,
            *list_of_df_building_kills,
            *list_of_df_monster_kills,
            *list_of_df_dragon_kills
        ]
    ).fill_null(0)  # Replace missing with zero

    df_snapshot = (
        df_events_up_to_snapshot
        .select([
            'matchId',
            'gameStartTimestamp',
            'patch',
            'timestamp',
            'platformId',
            'winningTeam',
            *[
                f"{column_prefix}{i}"
                for i in range(1, 11)
                for column_prefix in [
                    'currentGold_',
                    'inventoryIds_',
                    'inventoryCounts_',
                    'jungleMinionsKilled_',
                    'level_',
                    'minionsKilled_',
                    'rune_primary_0_',
                    'rune_primary_1_',
                    'rune_primary_2_',
                    'rune_primary_3_',
                    'rune_secondary_0_',
                    'rune_secondary_1_',
                    'rune_shard_defense_',
                    'rune_shard_flex_',
                    'rune_shard_offense_',
                    'positionX_',
                    'positionY_',
                    'summoner1Id_',
                    'summoner2Id_',
                    'totalGold_',
                    'xp_'
                ]
            ]
        ])
        .sort(["matchId", "timestamp"])
        .group_by("matchId", maintain_order=True)
        .last()
        .join(
            df_stats_at_snapshot,
            on="matchId",
            how="left"
        )
    )

    return df_snapshot
