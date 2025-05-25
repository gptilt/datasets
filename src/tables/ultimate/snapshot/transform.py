from functools import reduce
import polars as pl


def counts_from_events(
    df: pl.DataFrame,
    filter: pl.Expr,
    prefix: str
):
    df_counts = (
        df
        .filter(filter)
        .group_by(["matchId", "participantId"])
        .len()
        .pivot(
            on="participantId",
            index="matchId",
        )
    )
    return df_counts.rename({str(i): f"{prefix}_{i}" for i in df_counts.columns if i != "matchId"})


def snapshot_from_events(
    df_events_up_to_snapshot: pl.DataFrame
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
            'eventId',
            'timestamp',
            'platformId',
            *[
                f"{column_prefix}{i}"
                for i in range(1, 11)
                for column_prefix in [
                    'currentGold_',
                    'inventory_',
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
        .sort(["matchId", "eventId", "timestamp"])
        .group_by("matchId", maintain_order=True)
        .last()
        .join(
            df_stats_at_snapshot,
            on="matchId",
            how="left"
        )
    )
    
    return df_snapshot
