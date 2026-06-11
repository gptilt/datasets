"""
Clean-layer assets: latest snapshot of public figures, teams, and their aliases.

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


# ---- public_figures + person_aliases -----------------------------------------------


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


def _build_aliases(players: list[dict] | dict, redirects: list[dict] | dict) -> list[dict]:
    """
    Build `person_aliases` rows (alias → person_id). Names come from each Players
    record's own fields plus the `PlayerRedirects` table (alt-names / nicknames,
    which replaced the now-removed `Players.OtherNames` column). Both join on
    `OverviewPage`.
    """
    aliases: list[dict] = []
    for player in players:
        aliases.extend(_aliases_for_person(player["OverviewPage"], player))
    for redirect in redirects:
        alias = redirect.get("AllName")
        person_id = redirect.get("OverviewPage")
        if alias and person_id:
            aliases.append({"alias": alias, "person_id": person_id, "alias_type": "other"})

    # Dedup on (alias, person_id) — redirects and own-field names overlap.
    deduped = {(a["alias"], a["person_id"]): a for a in aliases}
    return list(deduped.values())


def _aliases_for_person(person_id: str, record: dict) -> list[dict]:
    """Emit one alias row per non-empty name field on a Players record."""
    rows: list[dict] = []
    if record.get("ID"):
        rows.append({"alias": record["ID"], "person_id": person_id, "alias_type": "ign"})
    if record.get("Name"):
        rows.append({"alias": record["Name"], "person_id": person_id, "alias_type": "real_name"})
    if record.get("NameAlphabet"):
        rows.append({"alias": record["NameAlphabet"], "person_id": person_id, "alias_type": "romanization"})
    if record.get("NativeName"):
        rows.append({"alias": record["NativeName"], "person_id": person_id, "alias_type": "romanization"})
    return rows


@dg.asset(
    name="clean_public_figures",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_leaguepedia_players"],
)
def asset_clean_public_figures(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    """
    Unified people table: pros + ex-pros + casters + coaches + analysts + owners.
    Overwrites the Iceberg table with the current partition's snapshot.
    """
    players, partition = _read_partition(
        esports_bucket, "leaguepedia_players", context.partition_key
    )

    figures = _build_figures(players)
    df = pl.DataFrame(figures)

    esports_catalog_clean.create_table_if_not_exists(
        "public_figures",
        schema=SCHEMATA["public_figures"]["schema"],
    )
    esports_catalog_clean.write_dataframe_to_table(
        "public_figures", df, mode="overwrite"
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(figures),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_person_aliases",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_leaguepedia_players", "raw_leaguepedia_player_redirects"],
)
def asset_clean_person_aliases(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    """
    Many-to-one alias table (alias → person_id). The privacy detector consumes this
    via `StorageIceberg.connect()` and builds an Aho-Corasick matcher from the
    `alias` column.
    """
    players, partition = _read_partition(
        esports_bucket, "leaguepedia_players", context.partition_key
    )
    redirects, _ = _read_partition(
        esports_bucket, "leaguepedia_player_redirects", context.partition_key
    )

    aliases = _build_aliases(players, redirects)
    df = pl.DataFrame(aliases)

    esports_catalog_clean.create_table_if_not_exists(
        "person_aliases",
        schema=SCHEMATA["person_aliases"]["schema"],
    )
    esports_catalog_clean.write_dataframe_to_table(
        "person_aliases", df, mode="overwrite"
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(aliases),
            "partition": dg.MetadataValue.json(partition),
        }
    )


# ---- teams + team_aliases ----------------------------------------------------------


def _build_teams_and_aliases(
    teams: list[dict] | dict,
) -> tuple[list[dict] | dict, list[dict]]:
    """
    Fan a Cargo `Teams` snapshot into `teams` rows and `team_aliases` rows.

    Cargo's team table has `Name` (full name, e.g. "T1"), `Short` (abbreviation,
    often equal to `Name`), `Region`, `Location`, and `IsDisbanded`. There's no
    separate long-name column anymore, so `long_name` stays null until `curated`.
    """
    rows: list[dict] = []
    aliases: list[dict] = []
    seen_team_ids: set[str] = set()

    for team in teams:
        team_id = team["PageName"]
        if team_id in seen_team_ids:
            continue
        seen_team_ids.add(team_id)
        canonical = team.get("Name") or team.get("Short") or team_id
        rows.append({
            "team_id": team_id,
            "canonical_name": canonical,
            "long_name": None,
            "region": team.get("Region"),
            "location": team.get("Location"),
            "active_from": None,
            "active_to": None,
            "is_disbanded": "Yes" if _is_truthy(team.get("IsDisbanded")) else "No",
            "source": "leaguepedia",
            "source_url": _wiki_url(team_id),
        })

        if team.get("Name"):
            aliases.append({"alias": team["Name"], "team_id": team_id, "alias_type": "short"})
        if team.get("Short") and team.get("Short") != team.get("Name"):
            aliases.append({"alias": team["Short"], "team_id": team_id, "alias_type": "short"})

    deduped = {(a["alias"], a["team_id"]): a for a in aliases}
    return rows, list(deduped.values())


@dg.asset(
    name="clean_teams",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_leaguepedia_teams"],
)
def asset_clean_teams(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    """
    Org-level team table: one row per Leaguepedia `Teams` page, deduped on the
    canonical `_pageName`. Overwrites the Iceberg target with this partition's
    snapshot — no history is retained at the clean layer.
    """
    teams, partition = _read_partition(
        esports_bucket, "leaguepedia_teams", context.partition_key
    )
    rows, _ = _build_teams_and_aliases(teams)
    df = pl.DataFrame(rows)

    esports_catalog_clean.create_table_if_not_exists(
        "teams",
        schema=SCHEMATA["teams"]["schema"],
    )
    esports_catalog_clean.write_dataframe_to_table("teams", df, mode="overwrite")
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_team_aliases",
    group_name="esports",
    partitions_def=partition_per_week,
    deps=["raw_leaguepedia_teams"],
)
def asset_clean_team_aliases(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    """
    Many-to-one alias table (alias → team_id) covering `Name`, `Short`, and
    `Long` from Cargo. Mirrors `clean_person_aliases` and feeds the privacy
    detector's team-name matcher.
    """
    teams, partition = _read_partition(
        esports_bucket, "leaguepedia_teams", context.partition_key
    )
    _, aliases = _build_teams_and_aliases(teams)
    df = pl.DataFrame(aliases)

    esports_catalog_clean.create_table_if_not_exists(
        "team_aliases",
        schema=SCHEMATA["team_aliases"]["schema"],
    )
    esports_catalog_clean.write_dataframe_to_table("team_aliases", df, mode="overwrite")
    return dg.MaterializeResult(
        metadata={
            "row_count": len(aliases),
            "partition": dg.MetadataValue.json(partition),
        }
    )
