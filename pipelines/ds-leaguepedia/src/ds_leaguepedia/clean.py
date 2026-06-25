"""
Clean-layer assets: latest snapshot of public figures, teams, and a unified
alias index over both.

Clean shares its partition definition with raw — both are weekly (Sunday-anchored).
The Iceberg target is overwritten every run: `clean` is "freshest snapshot wins",
no history. SCD Type 2 lives in `curated`.
"""
from urllib.parse import quote

import dagster as dg
import polars as pl
from ds_storage import StorageIceberg, StorageS3

from .partitions import partition_kwargs, partition_per_week
from .schemata import SCHEMATA


LEAGUEPEDIA_BASE = "https://lol.fandom.com/wiki/"


def _read_partition(
    bucket: StorageS3, raw_table_name: str, partition_key: str
) -> tuple[list | dict, dict]:
    """Fetch the raw snapshot written by the matching raw asset for this partition."""
    kwargs = partition_kwargs(partition_key)
    rows = bucket.get_object_as_json(
        raw_table_name,
        object_name=kwargs["week_of"],
        **kwargs,
    )
    return rows, kwargs


def _wiki_url(page_name: str) -> str:
    """Reconstruct a Leaguepedia page URL from a Cargo `_pageName` value."""
    return LEAGUEPEDIA_BASE + quote(page_name.replace(" ", "_"))


def _is_truthy(value: str | None) -> bool:
    """Cargo Boolean fields come back as '1'/'0' (older schemas used 'Yes'/'No').
    Treat the affirmatives as True; None/''/anything else is False."""
    return str(value).strip().lower() in {"1", "yes", "true"}


def _split_list(value: str | None) -> list[str] | None:
    """Cargo List fields are returned as comma-joined strings; split into a list.
    None/'' -> None. Champion/entity names never contain commas, so a plain split is safe."""
    if not value:
        return None
    return [item for item in value.split(",") if item]


# ---- public_figures ----------------------------------------------------------------


def _build_figures(players: list[dict] | dict) -> list[dict]:
    """
    Fan the unified `Players` table into `public_figures` rows. Players now covers
    everyone — pros, ex-pros, coaches, casters, analysts — so it's the single
    source. Identity is `OverviewPage`, Leaguepedia's cross-table page key.
    """
    figures: list[dict] = []
    seen_person_ids: set[str] = set()

    for player in players:
        person_id = player["OverviewPage"]
        if person_id in seen_person_ids:
            continue
        seen_person_ids.add(person_id)
        figures.append({
            "person_id": person_id,
            "canonical_name": player.get("Name") or player.get("ID") or person_id,
            "display_name": player.get("ID") or person_id,
            "role": player.get("Role"),
            "region": player.get("NationalityPrimary") or player.get("Country"),
            "active_from": None,
            "active_to": None,
            "is_retired": "Yes" if _is_truthy(player.get("IsRetired")) else "No",
            "source": "leaguepedia",
            "source_url": _wiki_url(person_id),
        })
    return figures


# ---- teams -------------------------------------------------------------------------


def _build_teams(teams: list[dict] | dict) -> list[dict]:
    """
    Fan a Cargo `Teams` snapshot into `teams` rows, deduped on the canonical
    `PageName`.

    Cargo's team table has `Name` (full name, e.g. "T1"), `Short` (abbreviation,
    often equal to `Name`), `Region`, `Location`, and `IsDisbanded`. There's no
    separate long-name column anymore, so `long_name` stays null until `curated`.
    """
    rows: list[dict] = []
    seen_team_ids: set[str] = set()

    for team in teams:
        team_id = team["PageName"]
        if team_id in seen_team_ids:
            continue
        seen_team_ids.add(team_id)
        rows.append({
            "team_id": team_id,
            "canonical_name": team.get("Name") or team.get("Short") or team_id,
            "long_name": None,
            "region": team.get("Region"),
            "location": team.get("Location"),
            "active_from": None,
            "active_to": None,
            "is_disbanded": "Yes" if _is_truthy(team.get("IsDisbanded")) else "No",
            "source": "leaguepedia",
            "source_url": _wiki_url(team_id),
        })
    return rows


# ---- games -------------------------------------------------------------------------


def _build_games(games: list[dict] | dict) -> list[dict]:
    """
    Fan a Cargo `ScoreboardGames` snapshot into `games` rows, deduped on `GameId`.
    Team1 is blue side, Team2 red (Leaguepedia convention); picks/bans are kept as the
    raw comma-separated strings Leaguepedia provides (consumers split).
    """
    rows: list[dict] = []
    seen: set[str] = set()
    for g in games:
        game_id = g.get("GameId")
        if not game_id or game_id in seen:
            continue
        seen.add(game_id)
        tournament = g.get("OverviewPage")
        rows.append({
            "game_id": game_id,
            "match_id": g.get("MatchId"),
            "tournament": tournament,
            # Cargo returns multi-word field names with spaces, not underscores.
            "game_number": g.get("N GameInMatch"),
            "datetime_utc": g.get("DateTime UTC"),
            "patch": g.get("Patch"),
            "team_blue": g.get("Team1"),
            "team_red": g.get("Team2"),
            "winner": g.get("WinTeam"),
            "picks_blue": _split_list(g.get("Team1Picks")),
            "picks_red": _split_list(g.get("Team2Picks")),
            "bans_blue": _split_list(g.get("Team1Bans")),
            "bans_red": _split_list(g.get("Team2Bans")),
            "gamelength": g.get("Gamelength"),
            "riot_game_id": g.get("RiotPlatformGameId"),
            "vod": g.get("VOD"),
            "source_url": _wiki_url(tournament) if tournament else None,
        })
    return rows


# ---- tournaments -------------------------------------------------------------------


def _build_tournaments(tournaments: list[dict] | dict) -> list[dict]:
    """Fan a Cargo `Tournaments` snapshot into `tournaments` rows, deduped on OverviewPage."""
    rows: list[dict] = []
    seen: set[str] = set()
    for t in tournaments:
        tournament_id = t.get("OverviewPage")
        if not tournament_id or tournament_id in seen:
            continue
        seen.add(tournament_id)
        rows.append({
            "tournament_id": tournament_id,
            "name": t.get("Name"),
            "league": t.get("League"),
            "region": t.get("Region"),
            "split": t.get("Split"),
            "year": t.get("Year"),
            "tier": t.get("TournamentLevel"),
            "date_start": t.get("DateStart"),
            "date_end": t.get("Date"),
            "source_url": _wiki_url(tournament_id),
        })
    return rows


# ---- matches -----------------------------------------------------------------------


def _build_matches(matches: list[dict] | dict) -> list[dict]:
    """Fan a Cargo `MatchSchedule` snapshot into `matches` (series) rows, deduped on MatchId."""
    rows: list[dict] = []
    seen: set[str] = set()
    for m in matches:
        match_id = m.get("MatchId")
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)
        tournament = m.get("OverviewPage")
        rows.append({
            "match_id": match_id,
            "tournament": tournament,
            "tab": m.get("Tab"),
            "team1": m.get("Team1"),
            "team2": m.get("Team2"),
            "team1_score": m.get("Team1Score"),
            "team2_score": m.get("Team2Score"),
            "winner": m.get("Winner"),
            "best_of": m.get("BestOf"),
            # Cargo returns the multi-word field name with a space, not an underscore.
            "datetime_utc": m.get("DateTime UTC"),
            "source_url": _wiki_url(tournament) if tournament else None,
        })
    return rows


# ---- entity_aliases (people + teams) -----------------------------------------------


def _person_aliases(players: list[dict] | dict, redirects: list[dict] | dict) -> list[dict]:
    """
    Alias rows for people (entity_type='person'). Names come from each Players
    record's own fields plus the `PlayerRedirects` table (alt-names / nicknames,
    which replaced the now-removed `Players.OtherNames` column). Both join on
    `OverviewPage`.
    """
    aliases: list[dict] = []
    for player in players:
        person_id = player["OverviewPage"]
        # Self-alias: the canonical id is always a resolvable surface form, so every
        # entity has at least one alias row and consumers can match on the id itself.
        aliases.append(_alias(person_id, person_id, "person", "canonical"))
        if player.get("ID"):
            aliases.append(_alias(player["ID"], person_id, "person", "ign"))
        if player.get("Name"):
            aliases.append(_alias(player["Name"], person_id, "person", "real_name"))
        if player.get("NameAlphabet"):
            aliases.append(_alias(player["NameAlphabet"], person_id, "person", "romanization"))
        if player.get("NativeName"):
            aliases.append(_alias(player["NativeName"], person_id, "person", "romanization"))
    for redirect in redirects:
        alias = redirect.get("AllName")
        person_id = redirect.get("OverviewPage")
        if alias and person_id:
            aliases.append(_alias(alias, person_id, "person", "other"))
    return aliases


def _team_aliases(teams: list[dict] | dict) -> list[dict]:
    """Alias rows for teams (entity_type='team') covering the canonical id, `Name`, and `Short`."""
    aliases: list[dict] = []
    for team in teams:
        team_id = team["PageName"]
        # Self-alias: the canonical id (PageName) is always a resolvable surface form —
        # `matches.team1/team2` carry it, so denormalized grounding can hit it directly.
        aliases.append(_alias(team_id, team_id, "team", "canonical"))
        if team.get("Name"):
            aliases.append(_alias(team["Name"], team_id, "team", "short"))
        if team.get("Short") and team.get("Short") != team.get("Name"):
            aliases.append(_alias(team["Short"], team_id, "team", "short"))
    return aliases


def _alias(alias: str, entity_id: str, entity_type: str, alias_type: str) -> dict:
    return {
        "alias": alias,
        "entity_id": entity_id,
        "entity_type": entity_type,
        "alias_type": alias_type,
    }


def _build_entity_aliases(
    players: list[dict] | dict,
    redirects: list[dict] | dict,
    teams: list[dict] | dict,
) -> list[dict]:
    """
    Unified surface-string → entity lookup over people and teams. People and teams
    are kept apart only by `entity_type`; the same string can legitimately map to
    both (so the dedup key is the full (alias, entity_id, entity_type) triple).
    """
    aliases = _person_aliases(players, redirects) + _team_aliases(teams)
    deduped = {(a["alias"], a["entity_id"], a["entity_type"]): a for a in aliases}
    return list(deduped.values())


# ---- assets ------------------------------------------------------------------------


@dg.asset(
    name="clean_esports_public_figures",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_esports_leaguepedia_players"],
)
def asset_clean_public_figures(
    context: dg.AssetExecutionContext,
    leaguepedia_bucket: StorageS3,
    leaguepedia_catalog_clean: StorageIceberg,
):
    """
    Unified people table: pros + ex-pros + casters + coaches + analysts + owners.
    Overwrites the Iceberg table with the current partition's snapshot.
    """
    players, partition = _read_partition(
        leaguepedia_bucket, "players", context.partition_key
    )

    figures = _build_figures(players)
    df = pl.DataFrame(figures)

    leaguepedia_catalog_clean.create_table_if_not_exists(
        "public_figures", SCHEMATA["public_figures"]
    )
    leaguepedia_catalog_clean.write_dataframe_to_table(
        "public_figures", df, mode="overwrite"
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(figures),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_esports_teams",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_esports_leaguepedia_teams"],
)
def asset_clean_teams(
    context: dg.AssetExecutionContext,
    leaguepedia_bucket: StorageS3,
    leaguepedia_catalog_clean: StorageIceberg,
):
    """
    Org-level team table: one row per Leaguepedia `Teams` page, deduped on the
    canonical `PageName`. Overwrites the Iceberg target with this partition's
    snapshot — no history is retained at the clean layer.
    """
    teams, partition = _read_partition(
        leaguepedia_bucket, "teams", context.partition_key
    )
    rows = _build_teams(teams)
    df = pl.DataFrame(rows)

    leaguepedia_catalog_clean.create_table_if_not_exists(
        "teams", SCHEMATA["teams"]
    )
    leaguepedia_catalog_clean.write_dataframe_to_table("teams", df, mode="overwrite")
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_esports_games",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_esports_leaguepedia_scoreboard_games"],
)
def asset_clean_games(
    context: dg.AssetExecutionContext,
    leaguepedia_bucket: StorageS3,
    leaguepedia_catalog_clean: StorageIceberg,
):
    """
    Per-game competitive record — one row per game. The grounding payload: a video
    identified to a game inherits its patch and champion comps. Overwrites the Iceberg
    target with this partition's snapshot.
    """
    games, partition = _read_partition(
        leaguepedia_bucket, "scoreboard_games", context.partition_key
    )
    rows = _build_games(games)
    df = pl.DataFrame(rows)

    leaguepedia_catalog_clean.create_table_if_not_exists("games", SCHEMATA["games"])
    leaguepedia_catalog_clean.write_dataframe_to_table("games", df, mode="overwrite")
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_esports_tournaments",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_esports_leaguepedia_tournaments"],
)
def asset_clean_tournaments(
    context: dg.AssetExecutionContext,
    leaguepedia_bucket: StorageS3,
    leaguepedia_catalog_clean: StorageIceberg,
):
    """One row per tournament. Overwrites the Iceberg target with this partition's snapshot."""
    tournaments, partition = _read_partition(
        leaguepedia_bucket, "tournaments", context.partition_key
    )
    rows = _build_tournaments(tournaments)
    df = pl.DataFrame(rows)

    leaguepedia_catalog_clean.create_table_if_not_exists("tournaments", SCHEMATA["tournaments"])
    leaguepedia_catalog_clean.write_dataframe_to_table("tournaments", df, mode="overwrite")
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_esports_matches",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_esports_leaguepedia_match_schedule"],
)
def asset_clean_matches(
    context: dg.AssetExecutionContext,
    leaguepedia_bucket: StorageS3,
    leaguepedia_catalog_clean: StorageIceberg,
):
    """One row per match/series. Overwrites the Iceberg target with this partition's snapshot."""
    matches, partition = _read_partition(
        leaguepedia_bucket, "match_schedule", context.partition_key
    )
    rows = _build_matches(matches)
    df = pl.DataFrame(rows)

    leaguepedia_catalog_clean.create_table_if_not_exists("matches", SCHEMATA["matches"])
    leaguepedia_catalog_clean.write_dataframe_to_table("matches", df, mode="overwrite")
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_esports_entity_aliases",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=[
        "raw_esports_leaguepedia_players",
        "raw_esports_leaguepedia_player_redirects",
        "raw_esports_leaguepedia_teams",
    ],
)
def asset_clean_entity_aliases(
    context: dg.AssetExecutionContext,
    leaguepedia_bucket: StorageS3,
    leaguepedia_catalog_clean: StorageIceberg,
):
    """
    Unified alias index (alias → entity) over both people and teams, discriminated
    by `entity_type`.
    """
    players, partition = _read_partition(
        leaguepedia_bucket, "players", context.partition_key
    )
    redirects, _ = _read_partition(
        leaguepedia_bucket, "player_redirects", context.partition_key
    )
    teams, _ = _read_partition(
        leaguepedia_bucket, "teams", context.partition_key
    )

    aliases = _build_entity_aliases(players, redirects, teams)
    df = pl.DataFrame(aliases)

    leaguepedia_catalog_clean.create_table_if_not_exists(
        "entity_aliases", SCHEMATA["entity_aliases"]
    )
    leaguepedia_catalog_clean.write_dataframe_to_table(
        "entity_aliases", df, mode="overwrite"
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(aliases),
            "partition": dg.MetadataValue.json(partition),
        }
    )
