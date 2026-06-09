"""
Clean-layer assets: latest snapshot of public figures, teams, and their aliases.

Each clean asset reads the most-recent raw snapshot for its source table from S3 and
overwrites the Iceberg table. No history is retained at this layer — `curated`
(future) is where SCD Type 2 lives.

The clean assets do NOT take the raw asset's return value as an input; they re-discover
the latest partition by listing S3. That makes the pipeline tolerant to clean and raw
running in different weeks (rerun, backfill, manual trigger) — clean always reads the
freshest snapshot it can find rather than failing if the current week's partition
hasn't been written yet.
"""
import re
from datetime import datetime
from urllib.parse import quote

import dagster as dg
import polars as pl
from ds_storage import StorageIceberg, StorageS3

from .schemata import SCHEMATA


LEAGUEPEDIA_BASE = "https://lol.fandom.com/wiki/"


def _latest_snapshot_partition(bucket: StorageS3, raw_table_name: str) -> dict:
    """
    Locate the most-recent `week_of=YYYYMMDD` partition for a raw table.

    Lists every object under the table prefix, extracts the `week_of` segment, and
    returns the partition kwargs for the latest one. Raises FileNotFoundError if no
    snapshot has been written yet (clean asset must run after at least one raw).
    """
    keys = bucket.list_objects(raw_table_name)
    if not keys:
        raise FileNotFoundError(
            f"No raw snapshots found for {raw_table_name}; materialise the raw asset first."
        )

    week_of_values = []
    for key in keys:
        match = re.search(r"week_of=(\d{8})", key)
        if match:
            week_of_values.append(match.group(1))

    if not week_of_values:
        raise FileNotFoundError(
            f"Snapshots for {raw_table_name} found but none had a week_of= partition segment."
        )

    latest = max(week_of_values)
    week_date = datetime.strptime(latest, "%Y%m%d")
    return {"year": week_date.year, "month": week_date.month, "week_of": latest}


def _read_latest_snapshot(bucket: StorageS3, raw_table_name: str) -> tuple[list[dict], dict]:
    """
    Fetch the latest scrape for a raw table along with its partition coordinates.

    Returns the raw rows plus the partition kwargs they came from — handy for asset
    metadata so we can see in the Dagster UI which snapshot was consumed.
    """
    partition = _latest_snapshot_partition(bucket, raw_table_name)
    rows = bucket.get_object_as_json(
        raw_table_name,
        object_name=partition["week_of"],
        **partition,
    )
    return rows, partition


def _wiki_url(page_name: str) -> str:
    """Reconstruct a Leaguepedia page URL from a Cargo `_pageName` value."""
    return LEAGUEPEDIA_BASE + quote(page_name.replace(" ", "_"))


def _split_other_names(other_names: str | None) -> list[str]:
    """`OtherNames` is a Cargo "list" field — values are comma-separated."""
    if not other_names:
        return []
    return [n.strip() for n in other_names.split(",") if n.strip()]


def _parse_iso_date(value: str | None) -> datetime | None:
    """Leaguepedia dates come as `YYYY-MM-DD`; everything else (None, '') → None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _is_truthy(value: str | None) -> bool:
    """Cargo boolean-like fields come as 'Yes' / 'No' / ''. Anything not 'Yes' is False."""
    return value == "Yes"


# ---- public_figures + person_aliases -----------------------------------------------


def _build_persons_and_aliases(
    players: list[dict], staff: list[dict]
) -> tuple[list[dict], list[dict]]:
    """
    Fan players + staff into `public_figures` rows and `person_aliases` rows.

    Players and staff share a name-shaped record: both have `_pageName` (canonical),
    `ID` (IGN / handle), `Name` (real name), `OtherNames` (alt aliases). Roles
    differ: a player's `Role` is a gameplay position; staff's `Occupation` is a
    profession. We unify those into a single `role` string so the privacy detector
    doesn't have to care about the source table.
    """
    figures: list[dict] = []
    aliases: list[dict] = []
    seen_person_ids: set[str] = set()

    for player in players:
        person_id = player["PageName"]
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
        aliases.extend(_aliases_for_person(person_id, player))

    for member in staff:
        person_id = member["PageName"]
        if person_id in seen_person_ids:
            # Person appears in both Players and Staff (e.g. retired pro turned coach).
            # Keep the player row but still pull additional aliases from the staff entry.
            aliases.extend(_aliases_for_person(person_id, member))
            continue
        seen_person_ids.add(person_id)
        figures.append({
            "person_id": person_id,
            "canonical_name": member.get("Name") or member.get("ID") or person_id,
            "display_name": member.get("ID") or person_id,
            "role": member.get("Occupation"),
            "region": member.get("Country"),
            "active_from": None,
            "active_to": None,
            "is_retired": "Yes" if _is_truthy(member.get("IsRetired")) else "No",
            "source": "leaguepedia",
            "source_url": _wiki_url(person_id),
        })
        aliases.extend(_aliases_for_person(person_id, member))

    # Dedup aliases on (alias, person_id) — `OtherNames` and `ID` can overlap.
    deduped = {(a["alias"], a["person_id"]): a for a in aliases}
    return figures, list(deduped.values())


def _aliases_for_person(person_id: str, record: dict) -> list[dict]:
    """Emit one alias row per non-empty name field on a Cargo Players/Staff record."""
    rows: list[dict] = []
    if record.get("ID"):
        rows.append({"alias": record["ID"], "person_id": person_id, "alias_type": "ign"})
    if record.get("Name"):
        rows.append({"alias": record["Name"], "person_id": person_id, "alias_type": "real_name"})
    if record.get("NameAlphabet"):
        rows.append({"alias": record["NameAlphabet"], "person_id": person_id, "alias_type": "romanization"})
    if record.get("NativeName"):
        rows.append({"alias": record["NativeName"], "person_id": person_id, "alias_type": "romanization"})
    for other in _split_other_names(record.get("OtherNames")):
        rows.append({"alias": other, "person_id": person_id, "alias_type": "other"})
    return rows


@dg.asset(
    name="clean_public_figures",
    group_name="esports",
    deps=["raw_leaguepedia_players", "raw_leaguepedia_staff"],
)
def asset_clean_public_figures(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    """
    Unified people table: pros + ex-pros + casters + coaches + analysts + owners.
    Overwrites the Iceberg table with the latest snapshot.
    """
    players, players_partition = _read_latest_snapshot(esports_bucket, "leaguepedia_players")
    staff, staff_partition = _read_latest_snapshot(esports_bucket, "leaguepedia_staff")

    figures, _ = _build_persons_and_aliases(players, staff)
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
            "players_partition": dg.MetadataValue.json(players_partition),
            "staff_partition": dg.MetadataValue.json(staff_partition),
        }
    )


@dg.asset(
    name="clean_person_aliases",
    group_name="esports",
    deps=["raw_leaguepedia_players", "raw_leaguepedia_staff"],
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
    players, players_partition = _read_latest_snapshot(esports_bucket, "leaguepedia_players")
    staff, staff_partition = _read_latest_snapshot(esports_bucket, "leaguepedia_staff")

    _, aliases = _build_persons_and_aliases(players, staff)
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
            "players_partition": dg.MetadataValue.json(players_partition),
            "staff_partition": dg.MetadataValue.json(staff_partition),
        }
    )


# ---- teams + team_aliases ----------------------------------------------------------


def _build_teams_and_aliases(
    teams: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Fan a Cargo `Teams` snapshot into `teams` rows and `team_aliases` rows.

    Cargo's team table has `Name` (canonical short, e.g. "T1"), `Long` (full name),
    `Short` (abbreviation, often equal to `Name`), and an optional `RenamedTo` chain
    pointing at the org's previous identity. We don't yet walk the rename chain —
    `curated.teams` will handle that history.
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
            "long_name": team.get("Long"),
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
        if team.get("Long"):
            aliases.append({"alias": team["Long"], "team_id": team_id, "alias_type": "long"})

    deduped = {(a["alias"], a["team_id"]): a for a in aliases}
    return rows, list(deduped.values())


@dg.asset(
    name="clean_teams",
    group_name="esports",
    deps=["raw_leaguepedia_teams"],
)
def asset_clean_teams(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    teams, partition = _read_latest_snapshot(esports_bucket, "leaguepedia_teams")
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
            "snapshot_partition": dg.MetadataValue.json(partition),
        }
    )


@dg.asset(
    name="clean_team_aliases",
    group_name="esports",
    deps=["raw_leaguepedia_teams"],
)
def asset_clean_team_aliases(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_catalog_clean: StorageIceberg,
):
    teams, partition = _read_latest_snapshot(esports_bucket, "leaguepedia_teams")
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
            "snapshot_partition": dg.MetadataValue.json(partition),
        }
    )
